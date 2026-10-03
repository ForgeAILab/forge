use std::{sync::Arc, time::Duration};

use db::SqliteDb;
use tokio::{sync::watch, task::JoinHandle};

use crate::Result;

// 100 pages every five seconds drains about 580 MB in two hours at 4 KiB/page.
// Each statement holds the writer lock for at most 100 page reclamations.
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(5);

pub struct StorageMaintenanceWorker {
    db: Arc<SqliteDb>,
}

impl StorageMaintenanceWorker {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    pub fn start(
        self: Arc<Self>,
        workers: &crate::worker_runtime::PeriodicWorkers,
        shutdown: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        workers.worker("storage-maintenance").start(shutdown, || false, move |worker, mut shutdown| {
            let maintenance = Arc::clone(&self);
            async move {
                if *shutdown.borrow_and_update() { return Ok(()); }
                let mut tick = tokio::time::interval(MAINTENANCE_INTERVAL);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow_and_update() { return Ok(()); }
                        }
                        _ = tick.tick() => {
                            let _ = worker.tick(maintenance.maintain_once(&shutdown)).await;
                        }
                    }
                }
            }
        })
    }

    async fn maintain_once(&self, shutdown: &watch::Receiver<bool>) -> Result<()> {
        if !*shutdown.borrow() {
            db::incremental_vacuum(self.db.pool()).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{create_sqlite_pool, run_migrations};

    async fn database() -> Arc<SqliteDb> {
        let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
        run_migrations(&pool).await.unwrap();
        sqlx::query("CREATE TABLE fixture (body BLOB)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("WITH RECURSIVE n(value) AS (SELECT 1 UNION ALL SELECT value+1 FROM n WHERE value<300) INSERT INTO fixture SELECT zeroblob(4096) FROM n").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM fixture")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type, scope_type, scope_id, correlation_id, created_at) VALUES ('old', 'test', 'test', 'test', 'system', 'system', 'system', 'old', '2000-01-01T00:00:00Z')").execute(&pool).await.unwrap();
        Arc::new(SqliteDb::new(pool))
    }

    #[tokio::test]
    async fn maintenance_is_bounded_and_preserves_old_events() {
        let db = database().await;
        let worker = StorageMaintenanceWorker::new(Arc::clone(&db));
        let (_tx, rx) = watch::channel(false);
        let before = db::sqlite_storage_status(db.pool())
            .await
            .unwrap()
            .free_pages;
        assert!(before > 100);
        worker.maintain_once(&rx).await.unwrap();
        let after = db::sqlite_storage_status(db.pool())
            .await
            .unwrap()
            .free_pages;
        assert_eq!(before - after, 100);
        let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_event WHERE id = 'old'")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(events, 1);
    }

    #[tokio::test]
    async fn maintenance_shutdown_leaves_free_pages_alone() {
        let db = database().await;
        let worker = Arc::new(StorageMaintenanceWorker::new(Arc::clone(&db)));
        let (_tx, rx) = watch::channel(true);
        let before = db::sqlite_storage_status(db.pool()).await.unwrap();
        worker.maintain_once(&rx).await.unwrap();
        Arc::clone(&worker)
            .start(
                &crate::worker_runtime::PeriodicWorkers::new(Arc::clone(&db)),
                rx,
            )
            .await
            .unwrap();
        assert_eq!(db::sqlite_storage_status(db.pool()).await.unwrap(), before);
    }

    #[tokio::test]
    async fn maintenance_is_a_noop_without_incremental_mode() {
        let db = database().await;
        sqlx::query("PRAGMA auto_vacuum = NONE")
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("VACUUM").execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO fixture VALUES (zeroblob(8192))")
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM fixture")
            .execute(db.pool())
            .await
            .unwrap();
        let before = db::sqlite_storage_status(db.pool()).await.unwrap();
        assert!(!before.incremental_vacuum && before.free_pages > 0);
        let (_tx, rx) = watch::channel(false);
        StorageMaintenanceWorker::new(Arc::clone(&db))
            .maintain_once(&rx)
            .await
            .unwrap();
        assert_eq!(db::sqlite_storage_status(db.pool()).await.unwrap(), before);
    }
}
