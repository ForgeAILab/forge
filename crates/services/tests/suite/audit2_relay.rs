//! Regression: can the read-only relay skip a sequence when
//! concurrent composite transactions commit on a file-backed database?
use db::{CreateDomainEvent, DomainEventRepo, SqliteDb};
use events::{EventBus, EventContext};
use services::DomainEventBroadcastConsumer;
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

fn event(id: String) -> CreateDomainEvent {
    CreateDomainEvent {
        id: id.clone(),
        event_type: "audit2".into(),
        entity_type: "project".into(),
        entity_id: "p".into(),
        actor_type: "system".into(),
        actor_id: None,
        scope_type: "project".into(),
        scope_id: "p".into(),
        correlation_id: id,
        causation_id: None,
        causation_depth: 0,
        dedupe_key: None,
        payload_json: "{}".into(),
        created_at: db::now_rfc3339(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn audit2_concurrent_composite_commits_never_leave_a_relay_gap() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("relay.db");
    let pool = db::create_sqlite_pool(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    let bus = Arc::new(EventBus::new(8192));
    let mut rx = bus.subscribe();
    let relay = Arc::new(DomainEventBroadcastConsumer::new(
        Arc::clone(&db),
        Arc::clone(&bus),
        Some(0),
    ));
    let (shutdown, signal) = watch::channel(false);
    let handle = Arc::clone(&relay).start(signal);

    const WRITERS: usize = 4;
    const PER_WRITER: usize = 150;
    let mut writers = Vec::new();
    for w in 0..WRITERS {
        let db = Arc::clone(&db);
        writers.push(tokio::spawn(async move {
            let mut rolled_back = 0usize;
            let mut busy = 0usize;
            for n in 0..PER_WRITER {
                // Alternate deferred and immediate composite transactions,
                // hold the write lock for a varying time, sometimes roll back.
                let deferred = (w + n) % 2 == 0;
                let mut tx = if deferred {
                    match db.pool().begin().await {
                        Ok(tx) => tx,
                        Err(_) => {
                            busy += 1;
                            continue;
                        }
                    }
                } else {
                    db::begin_immediate(db.pool()).await.unwrap()
                };
                match db
                    .append_event_in_tx(&mut tx, &event(format!("w{w}-n{n}")))
                    .await
                {
                    Ok(_) => {}
                    Err(_) => {
                        busy += 1;
                        let _ = tx.rollback().await;
                        continue;
                    }
                }
                tokio::time::sleep(Duration::from_micros(((w * 131 + n * 17) % 900) as u64)).await;
                if n % 11 == 3 {
                    tx.rollback().await.unwrap();
                    rolled_back += 1;
                } else if tx.commit().await.is_err() {
                    busy += 1;
                }
            }
            (rolled_back, busy)
        }));
    }
    let mut rolled_back = 0;
    let mut busy = 0;
    for writer in writers {
        let (r, b) = writer.await.unwrap();
        rolled_back += r;
        busy += b;
    }
    let committed: Vec<i64> =
        sqlx::query_scalar("SELECT sequence FROM domain_event ORDER BY sequence")
            .fetch_all(db.pool())
            .await
            .unwrap();
    let mut broadcast = Vec::new();
    for _ in &committed {
        let frame = tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .unwrap()
            .unwrap();
        if let EventContext::DomainEventCommitted { sequence, .. } = frame.context {
            broadcast.push(sequence);
        }
    }
    shutdown.send(true).unwrap();
    handle.await.unwrap();
    assert!(rx.try_recv().is_err(), "no duplicate frames after catch-up");
    println!(
        "audit2 relay: committed={} broadcast={} rolled_back={} busy={} max_seq={:?}",
        committed.len(),
        broadcast.len(),
        rolled_back,
        busy,
        committed.last()
    );
    assert!(committed.len() > 300, "enough commits to be meaningful");
    assert_eq!(
        broadcast, committed,
        "relay skipped or reordered a committed sequence"
    );
}
