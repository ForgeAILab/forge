//! Ordered durable event source; persistence and all SQL live in db.
use super::{Subscription, WorkerHealth};
use crate::Result;
use db::{DomainEvent, DomainEventRepo, SqliteDb};
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
            subscription: subscription.normalized(),
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
        Ok(self
            .db
            .initialize_event_worker_in_tx(tx, health, &self.subscription)
            .await?)
    }
    pub async fn is_initialized(&self) -> Result<bool> {
        Ok(self
            .db
            .event_worker_is_initialized(&self.name, &self.subscription)
            .await?)
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
        let (event, head) = self
            .db
            .scan_subscribed_domain_event(through, &self.subscription)
            .await?;
        if event.is_none() {
            self.scan.lock().expect("event scan lock").through = head.max(through);
        }
        Ok(event)
    }
    pub fn skip(&self, sequence: i64) {
        let mut scan = self.scan.lock().expect("event scan lock");
        scan.through = scan.through.max(sequence);
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
        let through = self.scan.lock().expect("event scan lock").through;
        Ok(self
            .db
            .validate_event_worker_in_tx(
                tx,
                &self.name,
                cursor,
                through,
                sequence,
                &self.subscription,
            )
            .await?)
    }
    pub async fn acknowledge_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        cursor: i64,
        sequence: i64,
    ) -> Result<()> {
        Ok(self
            .db
            .advance_domain_event_cursor_in_tx(tx, &self.name, cursor, sequence, &db::now_rfc3339())
            .await?)
    }
    pub async fn flush_in_tx(&self, tx: &mut Transaction<'_, Sqlite>, cursor: i64) -> Result<bool> {
        let through = self.scan.lock().expect("event scan lock").through;
        if through <= cursor {
            return Ok(false);
        }
        // Every row through this snapshot was either filtered in SQL or
        // classified Skip in this process. New appends have larger sequences.
        self.acknowledge_in_tx(tx, cursor, through).await?;
        let health = WorkerHealth::new(Arc::clone(&self.db), &self.name);
        self.db
            .complete_event_scan_in_tx(tx, &health, through)
            .await?;
        Ok(true)
    }
    pub fn flushed(&self) {
        self.scan.lock().expect("event scan lock").flushed_at = Instant::now();
    }
    #[cfg(test)]
    pub(super) fn make_flush_due(&self) {
        self.scan.lock().expect("event scan lock").flushed_at = Instant::now() - FLUSH_INTERVAL;
    }
}
