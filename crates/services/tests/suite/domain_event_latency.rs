use db::{CreateDomainEvent, DomainEventRepo, SqliteDb};
use events::{EventBus, EventContext};
use services::DomainEventBroadcastConsumer;
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, time::Instant};

#[tokio::test]
async fn audit_commit_to_broadcast_latency() {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    let bus = Arc::new(EventBus::new(1024));
    let mut rx = bus.subscribe();
    let relay = Arc::new(DomainEventBroadcastConsumer::new(
        Arc::clone(&db),
        Arc::clone(&bus),
        Some(0),
    ));
    let (shutdown, signal) = watch::channel(false);
    let handle = relay.start(signal);
    let mut samples = Vec::new();
    for n in 0..350 {
        let event = db
            .append_event(CreateDomainEvent {
                id: format!("latency-{n}"),
                event_type: "latency".into(),
                entity_type: "project".into(),
                entity_id: "p".into(),
                actor_type: "system".into(),
                actor_id: None,
                scope_type: "project".into(),
                scope_id: "p".into(),
                correlation_id: format!("latency-{n}"),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: None,
                payload_json: "{}".into(),
                created_at: db::now_rfc3339(),
            })
            .await
            .unwrap();
        let committed = Instant::now();
        loop {
            let received = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .unwrap()
                .unwrap();
            if matches!(received.context, EventContext::DomainEventCommitted { sequence, .. } if sequence == event.sequence)
            {
                break;
            }
        }
        if n >= 50 {
            samples.push(committed.elapsed().as_micros());
        }
    }
    samples.sort_unstable();
    println!(
        "commit-to-broadcast n={} p50={}us p95={}us",
        samples.len(),
        samples[samples.len() / 2],
        samples[samples.len() * 95 / 100]
    );
    shutdown.send(true).unwrap();
    handle.await.unwrap();
}
