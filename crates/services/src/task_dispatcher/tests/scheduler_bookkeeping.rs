//! Tests for scheduler bookkeeping in the task dispatcher.

use super::*;
#[tokio::test]
async fn supervised_dispatcher_recovers_panic_reports_budget_keeps_wakes_and_stops() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    struct InFlight(Arc<AtomicBool>);
    impl Drop for InFlight {
        fn drop(&mut self) {
            self.0.store(false, Ordering::SeqCst);
        }
    }
    let db = Arc::new(sqlite_db().await);
    let status_service = crate::OperatorStatusService::new_for_test(Arc::clone(&db));
    let root = TempDir::new().unwrap();
    let (mut built, _launches) = build_dispatcher(db, root.path()).await;
    built.instance_mut().check_interval = Duration::from_secs(300);
    let dispatcher = Arc::new(built.with_periodic_workers(status_service.periodic_workers()));
    let calls = Arc::new(AtomicUsize::new(0));
    let in_flight = Arc::new(AtomicBool::new(false));
    let resume = Arc::new(Notify::new());
    let success = Arc::new(Notify::new());
    let handle = Arc::clone(&dispatcher).start_with_check(Duration::from_millis(100), {
        let calls = Arc::clone(&calls);
        let in_flight = Arc::clone(&in_flight);
        let resume = Arc::clone(&resume);
        let success = Arc::clone(&success);
        move |_| {
            let calls = Arc::clone(&calls);
            let in_flight = Arc::clone(&in_flight);
            let resume = Arc::clone(&resume);
            let success = Arc::clone(&success);
            async move {
                match calls.fetch_add(1, Ordering::SeqCst) {
                    0 => panic!("dispatcher tick panic"),
                    1 => {
                        in_flight.store(true, Ordering::SeqCst);
                        let _in_flight = InFlight(in_flight);
                        resume.notified().await;
                    }
                    _ => {}
                }
                success.notify_one();
                // This test exercises the driver's wakes, independently of
                // any notifications check_once itself may emit in the future.
                Ok(0)
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = status_service.compute_status().await.unwrap();
            if status.recent_errors.iter().any(|row| {
                row.entity_type == "periodic_worker"
                    && row.entity_id == "task-dispatcher"
                    && row.error.contains("tick running longer than 100ms")
            }) {
                assert_eq!(
                    status.overall_severity,
                    api_types::OperatorSeverity::Attention
                );
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        in_flight.load(Ordering::SeqCst),
        "over-budget work was not dropped"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    resume.notify_one();
    tokio::time::timeout(Duration::from_secs(5), success.notified())
        .await
        .unwrap();
    dispatcher.task_service.dispatch_wake.notify_one();
    tokio::time::timeout(Duration::from_secs(5), success.notified())
        .await
        .unwrap();
    dispatcher.stop_notify.notify_one();
    tokio::time::timeout(Duration::from_secs(5), success.notified())
        .await
        .unwrap();
    dispatcher.stop();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .unwrap()
        .unwrap();
    let status = dispatcher.periodic_workers.status().await.unwrap();
    let row = status
        .iter()
        .find(|row| row.worker_name == "task-dispatcher")
        .unwrap();
    assert!(!row.running);
    assert!(row.last_tick_at.is_some());
    assert_eq!(row.restart_count, 1);
    assert!(row.last_error.is_none());
    assert!(!in_flight.load(Ordering::SeqCst));
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}

/// Run alone: the global SQLx subscriber measures only the dispatcher's calls.
#[tokio::test]
#[ignore = "one-minute dispatcher query/write measurement"]
async fn idle_load_5000_parked_tasks() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use tracing_subscriber::{layer::SubscriberExt, Layer};
    #[derive(Clone)]
    struct Counts(Arc<AtomicU64>, Arc<AtomicU64>);
    impl<S: tracing::Subscriber> Layer<S> for Counts {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Sql(Option<String>);
            impl tracing::field::Visit for Sql {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "db.statement" {
                        self.0 = Some(format!("{value:?}"));
                    }
                }
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    if field.name() == "db.statement" {
                        self.0 = Some(value.into());
                    }
                }
            }
            let mut sql = Sql(None);
            event.record(&mut sql);
            if let Some(statement) = sql.0 {
                let statement = statement.trim_start().to_ascii_uppercase();
                if statement.starts_with("SELECT") || statement.starts_with("WITH") {
                    self.0.fetch_add(1, Ordering::Relaxed);
                }
                if ["UPDATE", "INSERT", "DELETE", "REPLACE"]
                    .iter()
                    .any(|verb| statement.starts_with(verb))
                {
                    self.1.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
    let counts = Counts(Arc::default(), Arc::default());
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(counts.clone()))
        .unwrap();
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let seed = seed_task(&db, &project, "parked", "in_progress", 0).await;
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    sqlx::query(
        "UPDATE task SET blocked_json='{\"kind\":\"manual_stop\",\"reason\":\"held\"}' WHERE id=?",
    )
    .bind(&seed.id)
    .execute(&mut *tx)
    .await
    .unwrap();
    db.sync_condition_in_tx(&mut tx, &seed.id).await.unwrap();
    sqlx::query("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<4999) INSERT INTO task(id,project_id,title,task_type,status,priority,created_at,updated_at,blocked_json,condition_json) SELECT 'idle-'||i,project_id,title,task_type,status,priority,created_at,updated_at,blocked_json,json_set(condition_json,'$.evidence.witnesses[0].task_id','idle-'||i) FROM task JOIN n WHERE id=?")
        .bind(&seed.id).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let (dispatcher, _) = build_dispatcher_runtime(db, workspace.path(), false).await;
    dispatcher.check_once().await.unwrap();
    dispatcher.check_once().await.unwrap(); // Consume acknowledgements before measuring idle ticks.
    counts.0.store(0, Ordering::Relaxed);
    counts.1.store(0, Ordering::Relaxed);
    for _ in 0..6 {
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(dispatcher.check_once().await.unwrap(), 0);
    }
    println!(
        "IDLE_LOAD tasks=5000 seconds=60 queries={} write_statements={}",
        counts.0.load(Ordering::Relaxed),
        counts.1.load(Ordering::Relaxed)
    );
    counts.0.store(0, Ordering::Relaxed);
    counts.1.store(0, Ordering::Relaxed);
    dispatcher.schedule_state.lock().unwrap().sweep.due = std::time::Instant::now();
    dispatcher.reconcile_all().await.unwrap();
    println!(
        "SWEEP_LOAD tasks=5000 period=120 queries={} write_statements={}",
        counts.0.load(Ordering::Relaxed),
        counts.1.load(Ordering::Relaxed)
    );
}

#[tokio::test]
async fn unchanged_reconciliation_does_not_enqueue_a_second_step() {
    use db::TaskStepRepo;
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let agent = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project, "once", "in_progress", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    let (dispatcher, _) = build_dispatcher_runtime(db.clone(), workspace.path(), false).await;
    assert_eq!(dispatcher.check_once().await.unwrap(), 1);
    let before = db.task_steps(&task.id).await.unwrap().len();
    assert_eq!(dispatcher.check_once().await.unwrap(), 0);
    assert_eq!(db.task_steps(&task.id).await.unwrap().len(), before);
}
