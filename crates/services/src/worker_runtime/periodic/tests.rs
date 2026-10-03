use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;

async fn registry() -> PeriodicWorkers {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    PeriodicWorkers::new(Arc::new(SqliteDb::new(pool)))
}

async fn signal(notify: &Notify) {
    tokio::time::timeout(Duration::from_secs(5), notify.notified())
        .await
        .unwrap();
}

#[tokio::test]
async fn tick_panic_restarts_and_next_tick_runs_then_shutdown_stops() {
    let workers = registry().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(Notify::new());
    let (tx, rx) = watch::channel(false);
    let handle = workers.worker("periodic-panic").start(rx, || false, {
        let calls = Arc::clone(&calls);
        let completed = Arc::clone(&completed);
        move |worker, mut shutdown| {
            let calls = Arc::clone(&calls);
            let completed = Arc::clone(&completed);
            async move {
                worker
                    .tick(async {
                        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                            panic!("tick panic");
                        }
                        completed.notify_one();
                        Ok(())
                    })
                    .await?;
                while !*shutdown.borrow_and_update() {
                    if shutdown.changed().await.is_err() {
                        break;
                    }
                }
                Ok(())
            }
        }
    });
    signal(&completed).await;
    let status = workers.status().await.unwrap().remove(0);
    assert!(status.running);
    assert!(status.last_tick_at.is_some());
    assert_eq!(status.restart_count, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    tx.send(true).unwrap();
    handle.await.unwrap();
    assert!(!workers.status().await.unwrap()[0].running);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn timed_out_tick_is_dropped_recorded_and_next_tick_runs() {
    struct Cancelled(Arc<AtomicBool>);
    impl Drop for Cancelled {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let workers = registry().await;
    let cancelled = Arc::new(AtomicBool::new(false));
    let timed_out = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let completed = Arc::new(Notify::new());
    let (tx, rx) = watch::channel(false);
    let handle = workers
        .worker("periodic-timeout")
        .with_tick_timeout(Duration::from_millis(20))
        .start(rx, || false, {
            let cancelled = Arc::clone(&cancelled);
            let timed_out = Arc::clone(&timed_out);
            let resume = Arc::clone(&resume);
            let completed = Arc::clone(&completed);
            move |worker, mut shutdown| {
                let cancelled = Arc::clone(&cancelled);
                let timed_out = Arc::clone(&timed_out);
                let resume = Arc::clone(&resume);
                let completed = Arc::clone(&completed);
                async move {
                    let result = worker
                        .tick(async {
                            let _cancelled = Cancelled(cancelled);
                            std::future::pending::<Result<()>>().await
                        })
                        .await;
                    assert!(result.is_err());
                    timed_out.notify_one();
                    resume.notified().await;
                    worker.tick(async { Ok(()) }).await?;
                    completed.notify_one();
                    while !*shutdown.borrow_and_update() {
                        if shutdown.changed().await.is_err() {
                            break;
                        }
                    }
                    Ok(())
                }
            }
        });
    signal(&timed_out).await;
    assert!(cancelled.load(Ordering::SeqCst));
    let status = workers.status().await.unwrap().remove(0);
    assert!(status.last_error.unwrap().contains("tick timed out"));
    assert!(status.last_error_at.is_some());
    assert_eq!(status.restart_count, 0);
    resume.notify_one();
    signal(&completed).await;
    assert!(workers.status().await.unwrap()[0].last_error.is_none());
    tx.send(true).unwrap();
    handle.await.unwrap();
    assert!(!workers.status().await.unwrap()[0].running);
}

#[tokio::test]
async fn unexpected_return_restarts_but_atomic_stop_does_not() {
    let workers = registry().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let stopped = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(Notify::new());
    let handle = workers.worker("periodic-exit").start_stoppable(
        {
            let stopped = Arc::clone(&stopped);
            move || stopped.load(Ordering::SeqCst)
        },
        {
            let calls = Arc::clone(&calls);
            let stopped = Arc::clone(&stopped);
            let finished = Arc::clone(&finished);
            move |worker| {
                let calls = Arc::clone(&calls);
                let stopped = Arc::clone(&stopped);
                let finished = Arc::clone(&finished);
                async move {
                    worker
                        .tick(async {
                            if calls.fetch_add(1, Ordering::SeqCst) > 0 {
                                stopped.store(true, Ordering::SeqCst);
                                finished.notify_one();
                            }
                            Ok(())
                        })
                        .await
                }
            }
        },
    );
    signal(&finished).await;
    handle.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let status = workers.status().await.unwrap().remove(0);
    assert!(!status.running);
    assert_eq!(status.restart_count, 1);
}

#[tokio::test]
async fn shutdown_finishes_in_flight_tick_and_aborting_owner_drops_child() {
    let workers = registry().await;
    let entered = Arc::new(Notify::new());
    let finish = Arc::new(Notify::new());
    let (tx, rx) = watch::channel(false);
    let handle = workers.worker("periodic-grace").start(rx, || false, {
        let entered = Arc::clone(&entered);
        let finish = Arc::clone(&finish);
        move |worker, shutdown| {
            let entered = Arc::clone(&entered);
            let finish = Arc::clone(&finish);
            async move {
                worker
                    .tick(async {
                        entered.notify_one();
                        finish.notified().await;
                        Ok(())
                    })
                    .await?;
                assert!(*shutdown.borrow());
                Ok(())
            }
        }
    });
    signal(&entered).await;
    tx.send(true).unwrap();
    tokio::task::yield_now().await;
    assert!(!handle.is_finished());
    finish.notify_one();
    handle.await.unwrap();
    assert!(!workers.status().await.unwrap()[0].running);

    let (_tx, rx) = watch::channel(false);
    let handle = workers.worker("periodic-abort").start(rx, || false, {
        let entered = Arc::clone(&entered);
        move |worker, _| {
            let entered = Arc::clone(&entered);
            async move {
                worker
                    .tick(async {
                        entered.notify_one();
                        std::future::pending::<Result<()>>().await
                    })
                    .await
            }
        }
    });
    signal(&entered).await;
    handle.abort();
    assert!(handle.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), async {
        while workers
            .status()
            .await
            .unwrap()
            .iter()
            .any(|status| status.running)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
