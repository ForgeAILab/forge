use std::{sync::Arc, time::Duration};

use db::{SqliteDb, TaskStepRepo};
use tokio::{sync::watch, task::JoinHandle};

use crate::Result;

// 100 pages every five seconds drains about 580 MB in two hours at 4 KiB/page.
// Each statement holds the writer lock for at most 100 page reclamations.
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(5);
// Task step retention runs hourly inside the same worker, in bounded batches.
const STEP_PRUNE_INTERVAL: Duration = Duration::from_secs(3600);
const STEP_PRUNE_BATCH: i64 = 100;
const STEP_PRUNE_MAX_BATCHES: usize = 20;
const SETTLED_STEP_RETENTION_DAYS: i64 = 7;
const UNRESOLVED_STEP_RETENTION_DAYS: i64 = 30;

pub struct StorageMaintenanceWorker {
    db: Arc<SqliteDb>,
    last_step_prune: std::sync::Mutex<Option<tokio::time::Instant>>,
}

impl StorageMaintenanceWorker {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self {
            db,
            last_step_prune: std::sync::Mutex::new(None),
        }
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
                            if let Err(error) = worker.tick(maintenance.maintain_once(&shutdown)).await { tracing::warn!(worker = worker.name(), %error, "storage maintenance tick failed"); }
                        }
                    }
                }
            }
        })
    }

    async fn maintain_once(&self, shutdown: &watch::Receiver<bool>) -> Result<()> {
        if !*shutdown.borrow() {
            if self.step_prune_due() {
                self.prune_steps(shutdown).await?;
            }
            db::incremental_vacuum(self.db.pool()).await?;
        }
        Ok(())
    }

    fn step_prune_due(&self) -> bool {
        let mut last = self.last_step_prune.lock().expect("step prune clock");
        let now = tokio::time::Instant::now();
        if last.is_some_and(|at| now.duration_since(at) < STEP_PRUNE_INTERVAL) {
            return false;
        }
        *last = Some(now);
        true
    }

    /// Each batch is its own statement, so the writer lock is held for at
    /// most one 100-row delete per retention class.
    async fn prune_steps(&self, shutdown: &watch::Receiver<bool>) -> Result<u64> {
        let now = chrono::Utc::now();
        let settled = (now - chrono::Duration::days(SETTLED_STEP_RETENTION_DAYS)).to_rfc3339();
        let unresolved =
            (now - chrono::Duration::days(UNRESOLVED_STEP_RETENTION_DAYS)).to_rfc3339();
        let mut total = 0;
        for _ in 0..STEP_PRUNE_MAX_BATCHES {
            if *shutdown.borrow() {
                break;
            }
            let deleted = self
                .db
                .prune_steps(&settled, &unresolved, STEP_PRUNE_BATCH)
                .await?;
            total += deleted;
            if deleted < STEP_PRUNE_BATCH as u64 {
                break;
            }
        }
        Ok(total)
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
    async fn step_retention_runs_hourly_not_every_vacuum_tick() {
        let db = database().await;
        let now = db::now_rfc3339();
        sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES ('p','p',?,?)")
            .bind(&now)
            .bind(&now)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES ('t','p','t','todo',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        let old_step = |key: &str| {
            let key = key.to_owned();
            let db = Arc::clone(&db);
            async move {
                sqlx::query("INSERT INTO task_step(id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,status,available_at,created_at,updated_at,completed_at) VALUES (?,'t',(SELECT COALESCE(MAX(seq),0)+1 FROM task_step),'cascade','{}',?,?,1,'todo',1,'done','2000-01-01T00:00:00Z','2000-01-01T00:00:00Z','2000-01-01T00:00:00Z','2000-01-01T00:00:00Z')")
                    .bind(&key).bind(&key).bind(&key).execute(db.pool()).await.unwrap();
            }
        };
        let worker = StorageMaintenanceWorker::new(Arc::clone(&db));
        let (_tx, rx) = watch::channel(false);
        old_step("first").await;
        worker.maintain_once(&rx).await.unwrap();
        old_step("second").await;
        worker.maintain_once(&rx).await.unwrap();
        let left: Vec<String> = sqlx::query_scalar("SELECT id FROM task_step")
            .fetch_all(db.pool())
            .await
            .unwrap();
        assert_eq!(left, vec!["second".to_owned()]);
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
