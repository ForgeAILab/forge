//! Durable semantic-memory projection for singular Agent Chats.
//!
//! Chat admission and turn completion remain the source-of-truth writes.  This
//! consumer only claims durable domain events and writes the derived memory
//! item; an indexing failure leaves the event leased so a later process can
//! retry it after expiry.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use db::{
    now_rfc3339, AgentChatMessageRepo, AgentChatRepo, ClaimDomainEvents, CompleteDomainEvent,
    DomainEventRepo, SqliteDb,
};
use tokio::{sync::watch, task::JoinHandle, time::Duration as TokioDuration};
use uuid::Uuid;

use crate::{MemoryService, Result};

const CONSUMER_NAME: &str = "scoped-memory-agent-chat-indexer";

#[derive(Clone)]
pub struct AgentChatMemoryConsumer {
    db: Arc<SqliteDb>,
    memory: MemoryService<SqliteDb>,
    consumer_name: String,
    lease_owner: String,
}

impl AgentChatMemoryConsumer {
    pub fn new(db: Arc<SqliteDb>, lease_owner: impl Into<String>) -> Self {
        Self {
            memory: MemoryService::new(Arc::clone(&db)),
            db,
            consumer_name: CONSUMER_NAME.to_owned(),
            lease_owner: lease_owner.into(),
        }
    }

    pub fn with_consumer_name(mut self, consumer_name: impl Into<String>) -> Self {
        self.consumer_name = consumer_name.into();
        self
    }

    pub fn start(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(TokioDuration::from_secs(1));
            loop {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        if let Err(error) = self.run_once(100).await {
                            tracing::warn!(consumer = %self.consumer_name, %error, "Agent Chat memory consumer poll failed");
                        }
                    }
                }
            }
        })
    }

    pub async fn run_once(&self, limit: i64) -> Result<usize> {
        let now = now_rfc3339();
        let events = DomainEventRepo::claim_event_batch(
            &*self.db,
            ClaimDomainEvents {
                consumer_name: self.consumer_name.clone(),
                lease_owner: self.lease_owner.clone(),
                now: now.clone(),
                leased_until: lease_until(&now),
                limit: limit.clamp(1, 100),
            },
        )
        .await?;

        let mut processed = 0;
        for event in events {
            let should_index = matches!(
                event.event_type.as_str(),
                "agent_chat.message.admitted"
                    | "agent_chat.response.completed"
                    | "agent_chat.message.completed"
            ) && event.entity_type == "agent_chat_message";
            if should_index {
                if let Err(error) = self.index_agent_chat_event(&event).await {
                    tracing::warn!(
                        consumer = %self.consumer_name,
                        event_id = %event.id,
                        error = %error,
                        "Agent Chat memory indexing will retry"
                    );
                    continue;
                }
            }

            let dedupe_key = crate::domain_event_service::event_completion_dedupe_key(&event);
            DomainEventRepo::complete_claimed_event(
                &*self.db,
                CompleteDomainEvent {
                    consumer_name: self.consumer_name.clone(),
                    lease_owner: self.lease_owner.clone(),
                    event_sequence: event.sequence,
                    event_id: event.id,
                    dedupe_key,
                    completed_at: now_rfc3339(),
                },
            )
            .await?;
            processed += 1;
        }
        Ok(processed)
    }

    async fn index_agent_chat_event(&self, event: &db::DomainEvent) -> Result<()> {
        let chat = AgentChatRepo::get_agent_chat(&*self.db, &event.scope_id)
            .await?
            .ok_or(db::DbError::NotFound)?;
        let message = AgentChatMessageRepo::get_agent_chat_message(&*self.db, &event.entity_id)
            .await?
            .ok_or(db::DbError::NotFound)?;
        self.memory
            .record_agent_chat_message_event(event, &chat, &message)
            .await?;
        Ok(())
    }
}

fn lease_until(now: &str) -> String {
    DateTime::parse_from_rfc3339(now)
        .map(|value| (value.with_timezone(&Utc) + Duration::seconds(60)).to_rfc3339())
        .unwrap_or_else(|_| now.to_owned())
}

pub fn memory_consumer_name() -> &'static str {
    CONSUMER_NAME
}

pub fn memory_consumer_lease_owner() -> String {
    format!("agent-chat-memory-consumer-{}", Uuid::new_v4())
}
