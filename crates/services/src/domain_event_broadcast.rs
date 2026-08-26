//! Generic post-commit relay from the durable domain-event outbox to the
//! live SSE bus (D20: "every committed durable event must publish after its
//! transaction commits").
//!
//! Several commands append a domain event with
//! `DomainEventRepo::append_event_in_tx` from inside a larger composite
//! transaction — Project creation from a Charter approval, Main Genesis
//! control transfer, every Agent Chat message/turn, milestone
//! readiness/release — so the event row commits atomically with the records
//! it describes. That guarantees durability, but nothing then mirrors the
//! row to `EventBus`: the write is correct, it is just invisible to a
//! connected browser until its next poll or reconnect (the 8.4.2 audit
//! finding behind F16 — the client's named-listener bug was real, but even a
//! perfectly-routed client never saw these frames because the server never
//! sent one). This consumer is the missing "then": it claims every new
//! domain event exactly once, in commit order, and republishes it as a
//! `domain_event.committed` frame — the same generic wrapper
//! `DomainEventService::append` already uses for the events it publishes
//! itself.
//!
//! It is deliberately generic across event types and independent of any
//! bespoke `event_bus.publish` a command also makes directly (`project.
//! created`, `project.deleted`, `task.status_changed`, ...): those never
//! touch the `domain_event` table and are unaffected. An event that both
//! writes to the outbox *and* is published immediately by
//! `DomainEventService::append` is broadcast a second time here; the
//! client's invalidation is idempotent, so the redundancy costs one extra
//! broad-scoped refetch, never a correctness gap. `web/src/api/sse.ts`
//! routes `domain_event.committed` by the frame's `scope_type`/`entity_type`
//! to the exact query keys those scopes affect, falling back to a full
//! resync only for a scope it does not recognize.

use std::sync::Arc;

use chrono::{Duration, Utc};
use db::{now_rfc3339, ClaimDomainEvents, CompleteDomainEvent, DomainEventRepo, SqliteDb};
use events::EventBus;
use tokio::{sync::watch, task::JoinHandle, time::Duration as TokioDuration};
use uuid::Uuid;

use crate::{DomainEventService, Result};

const CONSUMER_NAME: &str = "sse-broadcast";
const LEASE_SECONDS: i64 = 30;
const POLL_INTERVAL: TokioDuration = TokioDuration::from_millis(500);
const BATCH_LIMIT: i64 = 100;

/// Drains the durable domain-event outbox and republishes every row to the
/// live `EventBus` exactly once, independent of which command wrote it.
pub struct DomainEventBroadcastConsumer {
    db: Arc<SqliteDb>,
    events: DomainEventService,
    lease_owner: String,
}

impl DomainEventBroadcastConsumer {
    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>) -> Self {
        Self::with_lease_owner(db, event_bus, domain_event_broadcast_lease_owner())
    }

    pub fn with_lease_owner(
        db: Arc<SqliteDb>,
        event_bus: Arc<EventBus>,
        lease_owner: impl Into<String>,
    ) -> Self {
        Self {
            events: DomainEventService::new(Arc::clone(&db), event_bus),
            db,
            lease_owner: lease_owner.into(),
        }
    }

    pub fn start(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut poll = tokio::time::interval(POLL_INTERVAL);
            poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow_and_update() {
                            break;
                        }
                    }
                    _ = poll.tick() => {
                        if let Err(error) = self.broadcast_once(BATCH_LIMIT).await {
                            tracing::warn!(error = %error, "SSE domain-event broadcast poll failed");
                        }
                    }
                }
            }
        })
    }

    /// Claim and republish a bounded batch, in commit order. Each event is
    /// checkpointed only after its publish returns, so a crash mid-batch
    /// leaves the remainder eligible for an idempotent replay rather than
    /// losing it.
    pub async fn broadcast_once(&self, limit: i64) -> Result<usize> {
        let now = now_rfc3339();
        let leased_until = (Utc::now() + Duration::seconds(LEASE_SECONDS)).to_rfc3339();
        let claimed = DomainEventRepo::claim_event_batch(
            &*self.db,
            ClaimDomainEvents {
                consumer_name: CONSUMER_NAME.to_owned(),
                lease_owner: self.lease_owner.clone(),
                now,
                leased_until,
                limit: limit.clamp(1, 100),
            },
        )
        .await?;

        let mut published = 0;
        for event in claimed {
            self.events.publish_committed(&event);
            let dedupe_key = crate::domain_event_service::event_completion_dedupe_key(&event);
            DomainEventRepo::complete_claimed_event(
                &*self.db,
                CompleteDomainEvent {
                    consumer_name: CONSUMER_NAME.to_owned(),
                    lease_owner: self.lease_owner.clone(),
                    event_sequence: event.sequence,
                    event_id: event.id,
                    dedupe_key,
                    completed_at: now_rfc3339(),
                },
            )
            .await?;
            published += 1;
        }
        Ok(published)
    }
}

pub fn domain_event_broadcast_consumer_name() -> &'static str {
    CONSUMER_NAME
}

pub fn domain_event_broadcast_lease_owner() -> String {
    format!("sse-broadcast-{}", Uuid::new_v4())
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{run_migrations, CreateDomainEvent};

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
        let consumer = DomainEventBroadcastConsumer::new(Arc::clone(&db), Arc::clone(&bus));

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
}
