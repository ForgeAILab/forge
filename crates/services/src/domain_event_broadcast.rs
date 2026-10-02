//! Read-only, in-memory tail of committed domain events for the live SSE bus.
use crate::{DomainEventService, Result};
use db::{DomainEventRepo, SqliteDb};
use events::EventBus;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{watch, Mutex},
    task::JoinHandle,
};
const CONSUMER_NAME: &str = "sse-broadcast";
const BATCH_LIMIT: i64 = 100;
const MIN_IDLE: Duration = Duration::from_millis(250);
const MAX_IDLE: Duration = Duration::from_secs(5);
pub struct DomainEventBroadcastConsumer {
    db: Arc<SqliteDb>,
    events: DomainEventService,
    position: Mutex<i64>,
}
impl DomainEventBroadcastConsumer {
    pub fn new(db: Arc<SqliteDb>, bus: Arc<EventBus>, after: i64) -> Self {
        Self {
            events: DomainEventService::new(Arc::clone(&db), bus),
            db,
            position: Mutex::new(after),
        }
    }
    pub(crate) async fn initialize_at_head(&self) -> Result<()> {
        *self.position.lock().await = self.db.domain_event_head().await?;
        Ok(())
    }
    pub fn start(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let notify = self.db.domain_event_notify();
            let mut idle = MIN_IDLE;
            loop {
                if *shutdown.borrow_and_update() {
                    return;
                }
                let notified = notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                match self.broadcast_once(BATCH_LIMIT).await {
                    Ok(n) if n > 0 => {
                        idle = MIN_IDLE;
                        continue;
                    }
                    Err(error) => tracing::warn!(%error, "SSE domain-event tail read failed"),
                    _ => {}
                }
                tokio::select! {
                    changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow_and_update() { return; } }
                    _ = &mut notified => {}
                    _ = tokio::time::sleep(idle) => {}
                }
                idle = idle.saturating_mul(2).min(MAX_IDLE);
            }
        })
    }
    pub async fn broadcast_once(&self, limit: i64) -> Result<usize> {
        let mut position = self.position.lock().await;
        let rows = self
            .db
            .list_events_after(*position, limit.clamp(1, BATCH_LIMIT))
            .await?;
        for row in &rows {
            self.events.publish_committed(row);
            *position = row.sequence;
        }
        Ok(rows.len())
    }
}
pub fn domain_event_broadcast_consumer_name() -> &'static str {
    CONSUMER_NAME
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{now_rfc3339, run_migrations, CreateDomainEvent};

    async fn database() -> Arc<SqliteDb> {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        run_migrations(&pool).await.unwrap();
        Arc::new(SqliteDb::new(pool))
    }

    #[tokio::test]
    async fn broadcasts_a_transactionally_written_event_exactly_once() {
        let db = database().await;
        let bus = Arc::new(EventBus::new(16));
        let mut rx = bus.subscribe();
        let consumer = DomainEventBroadcastConsumer::new(Arc::clone(&db), Arc::clone(&bus), 0);

        // Simulate the outbox pattern used by, e.g., project creation from a
        // Charter approval: the event row is inserted directly (standing in
        // for `append_event_in_tx` inside a larger composite transaction)
        // and only then is `broadcast_once` invoked, mirroring "after commit".
        let created = DomainEventRepo::append_event(
            &*db,
            CreateDomainEvent {
                id: db::new_uuid_v4(),
                event_type: "project.created_from_charter_approval".to_owned(),
                entity_type: "project".to_owned(),
                entity_id: "project-1".to_owned(),
                actor_type: "user".to_owned(),
                actor_id: Some("user-1".to_owned()),
                scope_type: "project".to_owned(),
                scope_id: "project-1".to_owned(),
                correlation_id: db::new_uuid_v4(),
                causation_id: None,
                causation_depth: 0,
                // Internal committed events may legitimately omit an explicit
                // key; their completion identity is the event id itself.
                dedupe_key: None,
                payload_json: "{}".to_owned(),
                created_at: now_rfc3339(),
            },
        )
        .await
        .expect("event appends");

        let published = consumer
            .broadcast_once(10)
            .await
            .expect("broadcast succeeds");
        assert_eq!(published, 1);

        let received = tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv())
            .await
            .expect("event arrives")
            .expect("channel open");
        assert_eq!(received.event_type, "domain_event.committed");
        assert_eq!(received.entity_id, created.id);

        // A second drain finds nothing left to claim: the cursor advanced
        // past this event, so it is never rebroadcast.
        let replayed = consumer
            .broadcast_once(10)
            .await
            .expect("broadcast succeeds");
        assert_eq!(replayed, 0);
    }
    #[tokio::test]
    async fn burst_is_ordered_without_gaps_duplicates_or_metadata_writes() {
        let db = database().await;
        let bus = Arc::new(EventBus::new(512));
        let mut rx = bus.subscribe();
        let consumer = DomainEventBroadcastConsumer::new(Arc::clone(&db), Arc::clone(&bus), 0);
        let mut expected = Vec::new();
        for n in 0..250 {
            let event = db
                .append_event(CreateDomainEvent {
                    id: format!("burst-{n}"),
                    event_type: "test".into(),
                    entity_type: "project".into(),
                    entity_id: "p".into(),
                    actor_type: "system".into(),
                    actor_id: None,
                    scope_type: "project".into(),
                    scope_id: "p".into(),
                    correlation_id: format!("burst-{n}"),
                    causation_id: None,
                    causation_depth: 0,
                    dedupe_key: None,
                    payload_json: "{}".into(),
                    created_at: now_rfc3339(),
                })
                .await
                .unwrap();
            expected.push(event.sequence);
        }
        let before: Vec<(String, i64, i64, String)> = sqlx::query_as("SELECT consumer_name, last_sequence, version, updated_at FROM event_consumer_cursor ORDER BY consumer_name").fetch_all(db.pool()).await.unwrap();
        assert_eq!(consumer.broadcast_once(100).await.unwrap(), 100);
        assert_eq!(consumer.broadcast_once(100).await.unwrap(), 100);
        assert_eq!(consumer.broadcast_once(100).await.unwrap(), 50);
        assert_eq!(consumer.broadcast_once(100).await.unwrap(), 0);
        let mut actual = Vec::new();
        while let Ok(frame) = rx.try_recv() {
            if let events::EventContext::DomainEventCommitted { sequence, .. } = frame.context {
                actual.push(sequence);
            }
        }
        assert_eq!(actual, expected);
        let after: Vec<(String, i64, i64, String)> = sqlx::query_as("SELECT consumer_name, last_sequence, version, updated_at FROM event_consumer_cursor ORDER BY consumer_name").fetch_all(db.pool()).await.unwrap();
        assert_eq!(before, after);
        let health: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM worker_health")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(health, 0);
    }
}
