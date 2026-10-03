use super::*;
use api_types::DeadLetterOutcome;
use db::{DeadLetter, DeadLetterAction};

/// Erases only the prepared/commit types; delivery stays in WorkerRuntime.
#[async_trait]
pub trait EventReplay: Send + Sync {
    async fn replay(
        &self,
        row: &DeadLetter,
        actor_id: &str,
    ) -> Result<(DeadLetter, DeadLetterOutcome)>;
}

pub(crate) async fn record_failed(
    db: &SqliteDb,
    row: &DeadLetter,
    actor_id: &str,
    error: WorkerError,
) -> Result<(DeadLetter, DeadLetterOutcome)> {
    let mut tx = db::begin_immediate(db.pool()).await?;
    db.fence_dead_letter_in_tx(&mut tx, row).await?;
    let updated = db
        .finish_dead_letter_in_tx(
            &mut tx,
            row,
            DeadLetterAction {
                actor_id,
                outcome: "replay_failed",
                reason: Some(error.message()),
                error_kind: Some(error.kind.as_str()),
            },
        )
        .await?;
    tx.commit().await?;
    Ok((updated, DeadLetterOutcome::ReplayFailed))
}

#[async_trait]
impl<W: Worker<C>, C: Send + Sync + 'static> EventReplay for WorkerRuntime<W, C> {
    async fn replay(
        &self,
        row: &DeadLetter,
        actor_id: &str,
    ) -> Result<(DeadLetter, DeadLetterOutcome)> {
        if !row.replayable() {
            return Err(db::DbError::DeadLetterNotReplayable.into());
        }
        if row.worker_name != self.worker.name() {
            return Err(ServiceError::invalid_operation(
                "dead letter belongs to another consumer",
            ));
        }
        let event = match row.event_sequence() {
            Some(sequence) => self.db.dead_letter_event(sequence).await?,
            None => None,
        };
        let Some(event) = event else {
            return record_failed(
                &self.db,
                row,
                actor_id,
                WorkerError::terminal("dead-letter source event unavailable"),
            )
            .await;
        };
        let subscribed = match self.worker.subscription() {
            Subscription::All => true,
            Subscription::Exact(types) => types.contains(&event.event_type),
            Subscription::Prefix(prefixes) => {
                prefixes.iter().any(|p| event.event_type.starts_with(p))
            }
        };
        if !subscribed {
            return record_failed(
                &self.db,
                row,
                actor_id,
                WorkerError::terminal("event no longer matches consumer subscription"),
            )
            .await;
        }
        // Same bounded re-preparation on a terminal commit error as normal delivery.
        for refresh in 0..2 {
            match self.prepare_delivery(&event).await {
                Ok(Outcome::Done(prepared)) => {
                    let mut tx = self.begin_write().await?;
                    self.db.fence_dead_letter_in_tx(&mut tx, row).await?;
                    self.health.ensure_in_tx(&mut tx).await?;
                    let committed = match self.commit_delivery(&mut tx, &event, &prepared).await {
                        Ok(value) => value,
                        Err(error) => {
                            tx.rollback().await?;
                            if error.kind == WorkerErrorKind::Terminal && refresh == 0 {
                                continue;
                            }
                            return record_failed(&self.db, row, actor_id, error).await;
                        }
                    };
                    let after_commit = self.db.get_dead_letter_in_tx(&mut tx, &row.id).await?;
                    if after_commit.version != row.version + 1 {
                        // A consumer can quarantine inside an otherwise successful
                        // commit. Roll back its effects and report that rejection.
                        let error = match after_commit.error_kind.as_str() {
                            "terminal" => WorkerError::terminal(after_commit.last_error),
                            "transient" => WorkerError::transient(after_commit.last_error),
                            _ => WorkerError::new(after_commit.last_error),
                        };
                        tx.rollback().await?;
                        return record_failed(&self.db, row, actor_id, error).await;
                    }
                    let updated = self
                        .db
                        .finish_dead_letter_in_tx(
                            &mut tx,
                            row,
                            DeadLetterAction {
                                actor_id,
                                outcome: "replayed",
                                reason: None,
                                error_kind: None,
                            },
                        )
                        .await?;
                    self.health.success_in_tx(&mut tx, &row.source_key).await?;
                    tx.commit().await?;
                    self.finish_delivery(&event, &prepared, &committed).await;
                    return Ok((updated, DeadLetterOutcome::Replayed));
                }
                Ok(Outcome::Skip) => {
                    let mut tx = self.begin_write().await?;
                    self.db.fence_dead_letter_in_tx(&mut tx, row).await?;
                    let updated = self
                        .db
                        .finish_dead_letter_in_tx(
                            &mut tx,
                            row,
                            DeadLetterAction {
                                actor_id,
                                outcome: "skipped",
                                reason: None,
                                error_kind: None,
                            },
                        )
                        .await?;
                    tx.commit().await?;
                    return Ok((updated, DeadLetterOutcome::Skipped));
                }
                Ok(Outcome::Defer { reason, .. }) => {
                    return record_failed(
                        &self.db,
                        row,
                        actor_id,
                        WorkerError::new(format!("replay deferred: {reason}")),
                    )
                    .await;
                }
                Ok(Outcome::DeadLetter { reason }) => {
                    return record_failed(&self.db, row, actor_id, WorkerError::terminal(reason))
                        .await;
                }
                Err(error) => return record_failed(&self.db, row, actor_id, error).await,
            }
        }
        unreachable!("fresh preparation returns a result")
    }
}
