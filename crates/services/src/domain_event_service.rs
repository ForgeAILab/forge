use std::sync::Arc;

use db::{CreateDomainEvent, DomainEvent, DomainEventRepo, SqliteDb};
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

    /// Call this only after the transaction containing `event` has committed.
    /// The bus payload intentionally excludes the authoritative event body.
    pub fn publish_committed(&self, event: &DomainEvent) {
        self.event_bus.publish(ForgeEvent {
            event_type: "domain_event.committed".to_owned(),
            entity_id: event.id.clone(),
            timestamp: event.created_at.clone(),
            context: EventContext::DomainEventCommitted {
                sequence: event.sequence,
                domain_event_type: event.event_type.clone(),
                domain_entity_id: event.entity_id.clone(),
                entity_type: event.entity_type.clone(),
                scope_type: event.scope_type.clone(),
                scope_id: event.scope_id.clone(),
            },
        });
    }
}
