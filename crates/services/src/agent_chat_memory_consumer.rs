//! Durable semantic-memory projection for singular Agent Chats.

use std::sync::Arc;

use async_trait::async_trait;
use db::{AgentChatMessageRepo, AgentChatRepo, DomainEvent, MemoryItem, SqliteDb};
use sqlx::{Sqlite, Transaction};
use tokio::{sync::watch, task::JoinHandle};

use crate::{
    worker_runtime::{Worker, WorkerError, WorkerOutcome, WorkerRuntime},
    MemoryService, Result,
};

const CONSUMER_NAME: &str = "scoped-memory-agent-chat-indexer";
const EVENT_TYPES: &[&str] = &[
    "agent_chat.message.admitted",
    "agent_chat.response.completed",
    "agent_chat.message.completed",
];

#[derive(Debug)]
pub struct PreparedMemoryProjection {
    item: Option<MemoryItem>,
    source_ref: String,
}

#[derive(Clone)]
pub struct AgentChatMemoryConsumer {
    db: Arc<SqliteDb>,
    memory: MemoryService<SqliteDb>,
    consumer_name: String,
}

impl AgentChatMemoryConsumer {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self {
            memory: MemoryService::new(Arc::clone(&db)),
            db,
            consumer_name: CONSUMER_NAME.to_owned(),
        }
    }

    pub fn with_consumer_name(mut self, consumer_name: impl Into<String>) -> Self {
        self.consumer_name = consumer_name.into();
        self
    }

    pub fn start(self: Arc<Self>, shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        let runtime = Arc::new(WorkerRuntime::new(Arc::clone(&self.db), self));
        runtime.start(shutdown)
    }

    pub async fn run_once(&self, limit: i64) -> Result<usize> {
        WorkerRuntime::new(Arc::clone(&self.db), Arc::new(self.clone()))
            .run_once(limit.clamp(1, 100) as usize)
            .await
    }
}

#[async_trait]
impl Worker for AgentChatMemoryConsumer {
    type Prepared = PreparedMemoryProjection;

    fn name(&self) -> &str {
        &self.consumer_name
    }

    fn event_types(&self) -> &'static [&'static str] {
        EVENT_TYPES
    }

    async fn handle(
        &self,
        event: &DomainEvent,
    ) -> std::result::Result<WorkerOutcome<Self::Prepared>, WorkerError> {
        if event.entity_type != "agent_chat_message" {
            return Ok(WorkerOutcome::Done(PreparedMemoryProjection {
                item: None,
                source_ref: event.entity_id.clone(),
            }));
        }
        let chat = AgentChatRepo::get_agent_chat(&*self.db, &event.scope_id)
            .await
            .map_err(|error| WorkerError::new(format!("Agent Chat lookup failed: {error}")))?
            .ok_or_else(|| WorkerError::new("Agent Chat source was not found"))?;
        let message = AgentChatMessageRepo::get_agent_chat_message(&*self.db, &event.entity_id)
            .await
            .map_err(|error| {
                WorkerError::new(format!("Agent Chat message lookup failed: {error}"))
            })?
            .ok_or_else(|| WorkerError::new("Agent Chat message source was not found"))?;
        let item = self
            .memory
            .prepare_agent_chat_message_event(event, &chat, &message)
            .map_err(|error| {
                WorkerError::new(format!("Agent Chat memory preparation failed: {error}"))
            })?;
        Ok(WorkerOutcome::Done(PreparedMemoryProjection {
            item,
            source_ref: message.id,
        }))
    }

    async fn commit(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        _event: &DomainEvent,
        prepared: &Self::Prepared,
    ) -> std::result::Result<(), WorkerError> {
        let Some(item) = prepared.item.as_ref() else {
            return Ok(());
        };
        self.db
            .insert_memory_item_if_source_absent_in_tx(
                transaction,
                item,
                "agent_chat",
                &prepared.source_ref,
            )
            .await
            .map_err(|error| {
                WorkerError::new(format!("Agent Chat memory write failed: {error}"))
            })?;
        Ok(())
    }
}

pub fn memory_consumer_name() -> &'static str {
    CONSUMER_NAME
}
