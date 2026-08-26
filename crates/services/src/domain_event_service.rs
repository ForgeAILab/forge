use std::{future::Future, sync::Arc};

use db::{
    now_rfc3339, ClaimDomainEvents, CompleteDomainEvent, CreateDomainEvent, DomainEvent,
    DomainEventRepo, SqliteDb,
};
use events::{EventBus, EventContext, ForgeEvent};

use crate::Result;

/// The repository's completion identity is the stored dedupe key when one is
/// present and the event id itself otherwise. Keep every durable consumer on
/// this exact fallback; inventing a prefixed value wedges its cursor forever
/// on legacy or internal events whose dedupe key is null.
pub(crate) fn event_completion_dedupe_key(event: &DomainEvent) -> String {
    event.dedupe_key.clone().unwrap_or_else(|| event.id.clone())
}

/// Commits authoritative domain events to SQLite and only then mirrors a
/// bounded invalidation notification to the in-process event bus.
#[derive(Clone)]
pub struct DomainEventService {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
}

impl DomainEventService {
    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>) -> Self {
        Self { db, event_bus }
    }

    pub async fn append(&self, input: CreateDomainEvent) -> Result<DomainEvent> {
        let event = DomainEventRepo::append_event(&*self.db, input).await?;
        self.publish_committed(&event);
        Ok(event)
    }

    pub async fn get(&self, id: &str) -> Result<Option<DomainEvent>> {
        Ok(DomainEventRepo::get_event(&*self.db, id).await?)
    }

    pub async fn get_by_dedupe(&self, dedupe_key: &str) -> Result<Option<DomainEvent>> {
        Ok(DomainEventRepo::get_event_by_dedupe(&*self.db, dedupe_key).await?)
    }

    pub async fn publish_by_dedupe(&self, dedupe_key: &str) -> Result<bool> {
        let Some(event) = self.get_by_dedupe(dedupe_key).await? else {
            return Ok(false);
        };
        self.publish_committed(&event);
        Ok(true)
    }

    pub async fn claim_batch(&self, input: ClaimDomainEvents) -> Result<Vec<DomainEvent>> {
        Ok(DomainEventRepo::claim_event_batch(&*self.db, input).await?)
    }

    /// Run a projection against one leased batch.  A failed handler leaves
    /// the lease/receipt untouched so the event is replayed after expiry.
    /// Completion happens only after the handler returns successfully.
    pub async fn process_batch<F, Fut>(
        &self,
        input: ClaimDomainEvents,
        mut handler: F,
    ) -> Result<usize>
    where
        F: FnMut(DomainEvent) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let consumer_name = input.consumer_name.clone();
        let lease_owner = input.lease_owner.clone();
        let completed_at = now_rfc3339();
        let events = self.claim_batch(input).await?;
        let mut completed = 0;
        for event in events {
            let dedupe_key = event_completion_dedupe_key(&event);
            handler(event.clone()).await?;
            let inserted = DomainEventRepo::complete_claimed_event(
                &*self.db,
                CompleteDomainEvent {
                    consumer_name: consumer_name.clone(),
                    lease_owner: lease_owner.clone(),
                    event_sequence: event.sequence,
                    event_id: event.id,
                    dedupe_key,
                    completed_at: completed_at.clone(),
                },
            )
            .await?;
            if inserted {
                completed += 1;
            }
        }
        Ok(completed)
    }

    /// Events are authoritative in SQLite; this helper is intentionally the
    /// only place that mirrors them to the process-local bus.
    pub fn should_wake_identity(event: &DomainEvent, identity_id: &str) -> bool {
        if event.causation_depth >= 16 {
            return false;
        }
        if event.actor_type == "agent" && event.actor_id.as_deref() == Some(identity_id) {
            return false;
        }
        let payload = serde_json::from_str::<serde_json::Value>(&event.payload_json).ok();
        !payload
            .as_ref()
            .and_then(|value| value.get("responder_identity_id"))
            .and_then(serde_json::Value::as_str)
            .is_some_and(|origin| origin == identity_id)
    }

    /// Call this only after the transaction containing `event` has committed.
    /// The bus payload intentionally excludes the authoritative event body.
    pub fn publish_committed(&self, event: &DomainEvent) {
        self.event_bus.publish(ForgeEvent {
            event_type: "domain_event.committed".to_owned(),
            entity_id: event.id.clone(),
            timestamp: event.created_at.clone(),
            context: EventContext::DomainEventCommitted {
                sequence: event.sequence,
                entity_type: event.entity_type.clone(),
                scope_type: event.scope_type.clone(),
                scope_id: event.scope_id.clone(),
            },
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(actor_type: &str, actor_id: Option<&str>, causation_depth: i64) -> DomainEvent {
        DomainEvent {
            sequence: 1,
            id: "event-1".to_owned(),
            event_type: "task.transitioned".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: "task-1".to_owned(),
            actor_type: actor_type.to_owned(),
            actor_id: actor_id.map(str::to_owned),
            scope_type: "task".to_owned(),
            scope_id: "task-1".to_owned(),
            correlation_id: "corr-1".to_owned(),
            causation_id: None,
            causation_depth,
            dedupe_key: Some("event-1".to_owned()),
            payload_json: "{}".to_owned(),
            created_at: "2026-08-12T20:00:00Z".to_owned(),
        }
    }

    #[test]
    fn self_events_and_depth_limit_do_not_wake_an_identity() {
        assert!(!DomainEventService::should_wake_identity(
            &event("agent", Some("agent-1"), 0),
            "agent-1"
        ));
        assert!(!DomainEventService::should_wake_identity(
            &event("system", None, 16),
            "agent-1"
        ));
        assert!(DomainEventService::should_wake_identity(
            &event("system", None, 15),
            "agent-1"
        ));
    }

    #[test]
    fn responder_origin_in_payload_is_self_suppressed() {
        let mut event = event("system", None, 0);
        event.payload_json = r#"{"responder_identity_id":"agent-1"}"#.to_owned();
        assert!(!DomainEventService::should_wake_identity(&event, "agent-1"));
    }
}
