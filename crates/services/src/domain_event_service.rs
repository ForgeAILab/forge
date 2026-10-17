use std::sync::Arc;

use db::{CreateDomainEvent, DomainEvent, DomainEventRepo, SqliteDb};
use events::{EventContext, ForgeEvent};

use crate::Result;

/// Commits authoritative domain events. The ordered relay alone mirrors them
/// to the in-process event bus after the pool commit hook wakes it.
#[derive(Clone)]
pub struct DomainEventService {
    db: Arc<SqliteDb>,
}

impl DomainEventService {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    pub async fn append(&self, input: CreateDomainEvent) -> Result<DomainEvent> {
        let event = DomainEventRepo::append_event(&*self.db, input).await?;
        Ok(event)
    }

    pub async fn get(&self, id: &str) -> Result<Option<DomainEvent>> {
        Ok(DomainEventRepo::get_event(&*self.db, id).await?)
    }

    pub async fn get_by_dedupe(&self, dedupe_key: &str) -> Result<Option<DomainEvent>> {
        Ok(DomainEventRepo::get_event_by_dedupe(&*self.db, dedupe_key).await?)
    }

    pub fn committed_frame(event: &DomainEvent) -> ForgeEvent {
        ForgeEvent {
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
        }
    }
}
