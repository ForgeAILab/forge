use std::{collections::HashMap, sync::Arc};

use api_types::{
    DeadLetterActionResponse, DeadLetterListResponse, DeadLetterOutcome, DeadLetterResponse,
    DeadLetterState, WorkerDeadLetterSummary,
};
use db::{DeadLetter, DeadLetterAction, SqliteDb};

use crate::{
    worker_runtime::{EventReplay, Worker, WorkerRuntime},
    ForgeRuntime, Result, ServiceError,
};

pub struct DeadLetterActor<'a> {
    pub user_id: &'a str,
    pub is_admin: bool,
}
impl DeadLetterActor<'_> {
    fn authorize(&self) -> Result<()> {
        if !self.is_admin || self.user_id.is_empty() {
            return Err(ServiceError::AuthorizationDenied {
                message: "Admin access required".into(),
            });
        }
        Ok(())
    }
}

pub struct DeadLetterService {
    db: Arc<SqliteDb>,
    consumers: HashMap<String, Arc<dyn EventReplay>>,
}
impl DeadLetterService {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self {
            db,
            consumers: HashMap::new(),
        }
    }
    pub fn for_runtime(runtime: &ForgeRuntime) -> Self {
        let mut service = Self::new(Arc::clone(&runtime.db));
        service.register(Arc::clone(&runtime.memory_consumer));
        service.register(Arc::clone(&runtime.coordination_consumer));
        service.register(Arc::clone(&runtime.attention_projection));
        service.register(Arc::clone(&runtime.wake_turn_consumer));
        service.register(Arc::clone(&runtime.project_hook_service));
        service.register(Arc::clone(&runtime.notification_service));
        service.register(Arc::clone(&runtime.conflict_hotspot_consumer));
        service
    }
    pub fn register<W: Worker<C>, C: Send + Sync + 'static>(&mut self, worker: Arc<W>) {
        self.consumers.insert(
            worker.name().to_owned(),
            Arc::new(WorkerRuntime::new(Arc::clone(&self.db), worker)),
        );
    }
    pub async fn list(
        &self,
        actor: DeadLetterActor<'_>,
        consumer: Option<&str>,
        state: DeadLetterState,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<DeadLetterListResponse> {
        actor.authorize()?;
        let page = self
            .db
            .list_dead_letters(
                consumer,
                matches!(state, DeadLetterState::Resolved),
                cursor,
                limit,
            )
            .await?;
        Ok(DeadLetterListResponse {
            items: page.items.into_iter().map(response).collect(),
            next_cursor: page.next_cursor,
        })
    }
    async fn open(&self, id: &str) -> Result<DeadLetter> {
        let row = self.db.get_dead_letter(id).await?;
        if row.resolved_at.is_some() {
            return Err(db::DbError::VersionConflict.into());
        }
        Ok(row)
    }
    pub async fn replay(
        &self,
        actor: DeadLetterActor<'_>,
        id: &str,
    ) -> Result<DeadLetterActionResponse> {
        actor.authorize()?;
        let row = self.db.get_dead_letter(id).await?;
        if !row.replayable() {
            return Err(db::DbError::DeadLetterNotReplayable.into());
        }
        if row.resolved_at.is_some() {
            return Err(db::DbError::VersionConflict.into());
        }
        let (updated, outcome) = if let Some(consumer) = self.consumers.get(&row.worker_name) {
            consumer.replay(&row, actor.user_id).await?
        } else {
            crate::worker_runtime::record_failed(
                &self.db,
                &row,
                actor.user_id,
                crate::worker_runtime::WorkerError::terminal(
                    "dead letter has no registered event consumer",
                ),
            )
            .await?
        };
        Ok(DeadLetterActionResponse {
            dead_letter: response(updated),
            outcome,
        })
    }
    pub async fn dismiss(
        &self,
        actor: DeadLetterActor<'_>,
        id: &str,
        reason: Option<&str>,
    ) -> Result<DeadLetterActionResponse> {
        actor.authorize()?;
        if reason.is_some_and(|reason| reason.chars().count() > 1024) {
            return Err(ServiceError::invalid_operation(
                "dismiss reason must be at most 1024 characters",
            ));
        }
        let reason = reason.map(|reason| {
            reason
                .chars()
                .filter(|c| !c.is_control())
                .collect::<String>()
        });
        let row = self.open(id).await?;
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        self.db.fence_dead_letter_in_tx(&mut tx, &row).await?;
        let updated = self
            .db
            .finish_dead_letter_in_tx(
                &mut tx,
                &row,
                DeadLetterAction {
                    actor_id: actor.user_id,
                    outcome: "dismissed",
                    reason: reason.as_deref(),
                    error_kind: None,
                },
            )
            .await?;
        tx.commit().await?;
        Ok(DeadLetterActionResponse {
            dead_letter: response(updated),
            outcome: DeadLetterOutcome::Dismissed,
        })
    }
}

pub(crate) fn summary(row: &DeadLetter) -> WorkerDeadLetterSummary {
    WorkerDeadLetterSummary {
        id: row.id.clone(),
        item_key: row.source_key.clone(),
        event_sequence: row.event_sequence(),
        replayable: row.replayable(),
        event_created_at: row.event_created_at.clone(),
        events_since: row.events_since,
        consumer_name: row.worker_name.clone(),
        event_type: row.item_type.clone(),
        attempts: row.attempts,
        reason: row.last_error.clone(),
        occurred_at: row.dead_lettered_at.clone(),
    }
}
fn response(row: DeadLetter) -> DeadLetterResponse {
    DeadLetterResponse {
        summary: summary(&row),
        state: if row.resolved_at.is_some() {
            DeadLetterState::Resolved
        } else {
            DeadLetterState::Open
        },
        error_kind: row.error_kind,
        first_failed_at: row.first_failed_at,
        last_failed_at: row.last_failed_at,
        resolved_at: row.resolved_at,
        resolved_by: row.resolved_by,
        resolution: row.resolution,
        resolution_reason: row.resolution_reason,
    }
}

#[cfg(test)]
mod tests;
