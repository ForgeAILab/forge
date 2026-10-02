//! Ordered durable event source and lazy ignored-event checkpointing.
use super::{Subscription, WorkerHealth};
use crate::{Result, ServiceError};
use db::{now_rfc3339, DomainEvent, DomainEventRepo, SqliteDb};
use sqlx::{Sqlite, Transaction};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;

const FLUSH_INTERVAL: Duration = Duration::from_secs(5);
struct Scan {
    cursor: i64,
    through: i64,
    flushed_at: Instant,
}
pub struct DurableEventSource {
    db: Arc<SqliteDb>,
    name: String,
    subscription: Subscription,
    scan: Mutex<Scan>,
}
impl DurableEventSource {
    pub fn new(db: Arc<SqliteDb>, name: impl Into<String>, subscription: Subscription) -> Self {
        Self {
            db,
            name: name.into(),
            subscription,
            scan: Mutex::new(Scan {
                cursor: 0,
                through: 0,
                flushed_at: Instant::now(),
            }),
        }
    }
    pub async fn initialize_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        health: &WorkerHealth,
    ) -> Result<()> {
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO event_consumer_cursor (consumer_name, last_sequence, version, updated_at)
            VALUES (?, 0, 1, ?) ON CONFLICT(consumer_name) DO NOTHING",
        )
        .bind(&self.name)
        .bind(&now)
        .execute(&mut **tx)
        .await?;
        health.ensure_in_tx(tx).await?;
        sqlx::query("UPDATE worker_health SET cursor_key = CAST((SELECT last_sequence FROM
            event_consumer_cursor WHERE consumer_name = ?) AS TEXT),
            cursor_updated_at = (SELECT updated_at FROM event_consumer_cursor WHERE consumer_name = ?),
            subscription_json = ? WHERE worker_name = ?")
            .bind(&self.name).bind(&self.name).bind(serde_json::to_string(&self.subscription)
                .map_err(|_| ServiceError::invalid_operation("invalid event subscription"))?)
            .bind(&self.name).execute(&mut **tx).await?;
        Ok(())
    }
    pub async fn is_initialized(&self) -> Result<bool> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(SELECT 1 FROM worker_health AS health
            JOIN event_consumer_cursor AS cursor ON health.worker_name = cursor.consumer_name
            WHERE worker_name = ? AND subscription_json IS NOT NULL)",
        )
        .bind(&self.name)
        .fetch_one(self.db.pool())
        .await?
            != 0)
    }
    pub async fn cursor(&self) -> Result<i64> {
        Ok(self
            .db
            .get_consumer_cursor(&self.name)
            .await?
            .ok_or(db::DbError::NotFound)?
            .last_sequence)
    }
    pub async fn next(&self, cursor: i64) -> Result<Option<DomainEvent>> {
        let through = {
            let mut scan = self.scan.lock().expect("event scan lock");
            if scan.cursor != cursor {
                scan.cursor = cursor;
                scan.through = cursor;
            }
            scan.through.max(cursor)
        };
        let event = self
            .db
            .next_subscribed_domain_event(through, &self.subscription)
            .await?;
        if event.is_none() {
            // Use only the scan's previous snapshot: a wanted append racing
            // the first lookup must never be included in an ignored advance.
            let head: i64 =
                sqlx::query_scalar("SELECT COALESCE(MAX(sequence), 0) FROM domain_event")
                    .fetch_one(self.db.pool())
                    .await?;
            // Recheck through that head before marking it scanned.
            if let Some(event) = self
                .db
                .next_subscribed_domain_event(through, &self.subscription)
                .await?
            {
                return Ok(Some(event));
            }
            self.scan.lock().expect("event scan lock").through = head.max(through);
        }
        Ok(event)
    }
    pub fn flush_due(&self, cursor: i64) -> bool {
        let scan = self.scan.lock().expect("event scan lock");
        scan.through > cursor && scan.flushed_at.elapsed() >= FLUSH_INTERVAL
    }
    pub async fn validate_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        cursor: i64,
        sequence: i64,
    ) -> Result<()> {
        let current: i64 = sqlx::query_scalar(
            "SELECT last_sequence FROM event_consumer_cursor WHERE consumer_name = ?",
        )
        .bind(&self.name)
        .fetch_one(&mut **tx)
        .await?;
        let next = self
            .db
            .next_subscribed_domain_event_sequence_in_tx(tx, current, &self.subscription)
            .await?;
        if current != cursor || next != Some(sequence) {
            return Err(db::DbError::VersionConflict.into());
        }
        Ok(())
    }
    pub async fn acknowledge_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        cursor: i64,
        sequence: i64,
    ) -> Result<()> {
        let now = now_rfc3339();
        self.db
            .advance_domain_event_cursor_in_tx(tx, &self.name, cursor, sequence, &now)
            .await?;
        sqlx::query("UPDATE worker_health SET cursor_key = ?, cursor_updated_at = ?, updated_at = ? WHERE worker_name = ?")
            .bind(sequence.to_string()).bind(&now).bind(&now).bind(&self.name).execute(&mut **tx).await?;
        Ok(())
    }
    pub async fn flush_in_tx(&self, tx: &mut Transaction<'_, Sqlite>, cursor: i64) -> Result<bool> {
        let through = self.scan.lock().expect("event scan lock").through;
        let next = self
            .db
            .next_subscribed_domain_event_sequence_in_tx(tx, cursor, &self.subscription)
            .await?;
        let target = next.map_or(through, |n| through.min(n - 1));
        if target <= cursor {
            return Ok(false);
        }
        self.acknowledge_in_tx(tx, cursor, target).await?;
        Ok(true)
    }
    pub fn flushed(&self) {
        self.scan.lock().expect("event scan lock").flushed_at = Instant::now();
    }
}
