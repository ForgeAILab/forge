use crate::{DbError, Result, SqliteStorageStatus};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Connection, Sqlite, SqliteConnection, SqlitePool, Transaction,
};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard, OnceLock, Weak,
    },
    time::Duration,
};
use tokio::sync::Notify;

#[derive(Debug, Default)]
pub(crate) struct EventHooks {
    committed: Mutex<HashSet<usize>>,
    pending: AtomicUsize,
    notify: Arc<Notify>,
    #[cfg(test)]
    release_inspections: AtomicUsize,
}
impl EventHooks {
    fn guard(&self) -> MutexGuard<'_, HashSet<usize>> {
        match self.committed.lock() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        }
    }
    fn mark(&self, key: usize) {
        let mut committed = self.guard();
        committed.insert(key);
        self.pending.store(committed.len(), Ordering::Release);
    }
    fn take(&self, key: usize) -> bool {
        let mut committed = self.guard();
        let removed = committed.remove(&key);
        self.pending.store(committed.len(), Ordering::Release);
        removed
    }
    pub(crate) fn notify(&self) -> Arc<Notify> {
        Arc::clone(&self.notify)
    }
}
fn notifier_registry() -> &'static Mutex<HashMap<usize, Weak<EventHooks>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<usize, Weak<EventHooks>>>> = OnceLock::new();
    REGISTRY.get_or_init(Mutex::default)
}
fn registry_guard() -> MutexGuard<'static, HashMap<usize, Weak<EventHooks>>> {
    match notifier_registry().lock() {
        Ok(guard) => guard,
        Err(poison) => poison.into_inner(),
    }
}
fn pool_key(pool: &SqlitePool) -> usize {
    Arc::as_ptr(&pool.connect_options()) as usize
}
pub(crate) fn domain_event_hooks(pool: &SqlitePool) -> Arc<EventHooks> {
    let mut registry = registry_guard();
    if let Some(hooks) = registry.get(&pool_key(pool)).and_then(Weak::upgrade) {
        return hooks;
    }
    let hooks = Arc::new(EventHooks::default());
    registry.insert(pool_key(pool), Arc::downgrade(&hooks));
    hooks
}

pub async fn create_sqlite_pool(database_url: &str) -> Result<SqlitePool> {
    let max_connections = if database_url.contains(":memory:") {
        1
    } else {
        5
    };
    let options = SqliteConnectOptions::from_str(database_url)?.create_if_missing(true);

    // Connection identities are opaque keys only; no pointer is dereferenced.
    // SQLite's commit hook is BEFORE visibility. It only marks a connection;
    // SQLx release delivers the notification AFTER the commit has completed.
    let hooks = Arc::new(EventHooks::default());
    let on_connect = Arc::clone(&hooks);
    let on_release = Arc::clone(&hooks);
    let pool = SqlitePoolOptions::new()
        .max_connections(max_connections)
        // A release must pass through the notification hook rather than the
        // lifetime-expiry close path. Idle connections can still be recycled.
        .max_lifetime(None)
        .acquire_timeout(Duration::from_secs(30))
        .after_connect(move |connection, _metadata| {
            let hooks = Arc::clone(&on_connect);
            Box::pin(async move {
                // Only empty databases can enable auto-vacuum without a full
                // rebuild. Set this before WAL, which dirties even an empty
                // file. Never change an existing database's mode here.
                let tables: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
                ).fetch_one(&mut *connection).await?;
                if tables == 0 {
                    sqlx::query("PRAGMA auto_vacuum = INCREMENTAL")
                        .execute(&mut *connection).await?;
                }

                sqlx::query("PRAGMA foreign_keys = ON")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("PRAGMA recursive_triggers = ON")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("PRAGMA journal_mode = WAL")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("PRAGMA synchronous = NORMAL")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("PRAGMA busy_timeout = 30000")
                    .execute(&mut *connection)
                    .await?;
                let dirty = Arc::new(AtomicBool::new(false));
                let mut handle = connection.lock_handle().await?;
                let key = handle.as_raw_handle().as_ptr() as usize;
                hooks.take(key);
                let updated = Arc::clone(&dirty);
                handle.set_update_hook(move |update| {
                    if update.table == "domain_event" && update.operation == sqlx::sqlite::SqliteOperation::Insert {
                        updated.store(true, Ordering::Relaxed);
                    }
                });
                let rollback_dirty = Arc::clone(&dirty);
                handle.set_rollback_hook(move || {
                    rollback_dirty.store(false, Ordering::Relaxed);
                    // Preserve an earlier committed append on a connection
                    // retained across transactions. A failed COMMIT can cause
                    // a harmless extra wake, never a lost committed append.
                });
                handle.set_commit_hook(move || {
                    if dirty.swap(false, Ordering::Relaxed) {
                        hooks.mark(key);
                    }
                    true
                });
                Ok(())
            })
        })
        .after_release(move |connection, _metadata| {
            let hooks = Arc::clone(&on_release);
            Box::pin(async move {
                // The common read/non-event-write path needs neither a ping nor
                // a trip to the SQLite worker. A marker is published in the
                // pre-commit hook; delivery waits for this committed release.
                if hooks.pending.load(Ordering::Acquire) == 0 { return Ok(true); }
                #[cfg(test)] hooks.release_inspections.fetch_add(1, Ordering::Relaxed);
                // lock_handle queues behind any outstanding rollback/COMMIT;
                // SQLx's own release check subsequently pings the connection.
                let key = connection.lock_handle().await?.as_raw_handle().as_ptr() as usize;
                if hooks.take(key) { hooks.notify.notify_waiters(); }
                Ok(true)
            })
        })
        .connect_with(options)
        .await?;

    registry_guard().insert(pool_key(&pool), Arc::downgrade(&hooks));
    Ok(pool)
}

/// Begins a write transaction with `BEGIN IMMEDIATE`, acquiring the SQLite
/// write lock up front. A plain (deferred) `BEGIN` only upgrades to a write
/// lock lazily, on the first write statement — and that upgrade does not
/// honor `busy_timeout`, so under contention it fails instantly with
/// SQLITE_BUSY_SNAPSHOT instead of retrying. Use this for any transaction
/// that performs writes.
///
/// Returns `sqlx::Result` (not `crate::Result`) so it is a drop-in
/// replacement for `pool.begin()` at every call site, including outside the
/// `db` crate, without changing error-conversion paths.
pub async fn begin_immediate(pool: &SqlitePool) -> sqlx::Result<Transaction<'static, Sqlite>> {
    pool.begin_with("BEGIN IMMEDIATE").await
}

/// Operator diagnostics for the database's persistent vacuum mode and freelist.
pub async fn sqlite_storage_status(pool: &SqlitePool) -> Result<SqliteStorageStatus> {
    let mut conn = pool.acquire().await?;
    let mode: i64 = sqlx::query_scalar("PRAGMA auto_vacuum")
        .fetch_one(&mut *conn)
        .await?;
    let free_pages = sqlx::query_scalar("PRAGMA freelist_count")
        .fetch_one(&mut *conn)
        .await?;
    Ok(SqliteStorageStatus {
        incremental_vacuum: mode == 2,
        free_pages,
    })
}

/// Release at most 100 free pages per maintenance pass. This is deliberately a
/// no-op on existing databases until the operator converts them offline.
pub async fn incremental_vacuum(pool: &SqlitePool) -> Result<()> {
    let mut conn = pool.acquire().await?;
    let mode: i64 = sqlx::query_scalar("PRAGMA auto_vacuum")
        .fetch_one(&mut *conn)
        .await?;
    if mode != 2 {
        return Ok(());
    }
    // Skip the write lock entirely when there is nothing to release.
    let free_pages: i64 = sqlx::query_scalar("PRAGMA freelist_count")
        .fetch_one(&mut *conn)
        .await?;
    if free_pages > 0 {
        // SQLite returns one row per vacuum step; consume all rows to finish
        // the bounded statement rather than stopping after its first step.
        sqlx::query("PRAGMA incremental_vacuum(100)")
            .fetch_all(&mut *conn)
            .await?;
    }
    Ok(())
}

/// One-time offline conversion. The caller must hold the data-root runtime
/// lock. A dedicated exclusive connection prevents other SQLite connections
/// from accessing the file during the full VACUUM, with no busy retry.
pub async fn convert_sqlite_to_incremental(database_url: &str) -> Result<()> {
    let options = SqliteConnectOptions::from_str(database_url)?
        .create_if_missing(false)
        .busy_timeout(Duration::ZERO);
    let mut conn = SqliteConnection::connect_with(&options).await?;
    sqlx::query("PRAGMA locking_mode = EXCLUSIVE")
        .execute(&mut conn)
        .await?;
    sqlx::query("BEGIN EXCLUSIVE").execute(&mut conn).await?;
    sqlx::query("COMMIT").execute(&mut conn).await?;
    let mode: i64 = sqlx::query_scalar("PRAGMA auto_vacuum")
        .fetch_one(&mut conn)
        .await?;
    if mode != 2 {
        sqlx::query("PRAGMA auto_vacuum = INCREMENTAL")
            .execute(&mut conn)
            .await?;
        sqlx::query("VACUUM").execute(&mut conn).await?;
    }
    let mode: i64 = sqlx::query_scalar("PRAGMA auto_vacuum")
        .fetch_one(&mut conn)
        .await?;
    if mode != 2 {
        return Err(DbError::Check(
            "database did not enter incremental auto-vacuum mode".to_owned(),
        ));
    }
    conn.close().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::AssertUnwindSafe;

    async fn no_signal(signal: &Notify) {
        assert!(
            tokio::time::timeout(Duration::from_secs(1), signal.notified())
                .await
                .is_err()
        );
    }
    async fn fixture(path: &std::path::Path) -> SqlitePool {
        let pool = create_sqlite_pool(&format!("sqlite://{}", path.display()))
            .await
            .unwrap();
        sqlx::raw_sql("CREATE TABLE domain_event(id INTEGER PRIMARY KEY, v TEXT UNIQUE); CREATE TABLE other(v TEXT);")
            .execute(&pool).await.unwrap();
        pool
    }
    // Ports W1-W5 without wall-clock performance thresholds. The fast-path
    // counter proves ordinary pool users take no additional worker round trip.
    #[tokio::test]
    async fn event_hooks_preserve_commits_isolate_pools_and_skip_unmarked_releases() {
        let dir = tempfile::tempdir().unwrap();
        let pool = fixture(&dir.path().join("events.sqlite")).await;
        let other_pool = fixture(&dir.path().join("other.sqlite")).await;
        let hooks = domain_event_hooks(&pool);
        let other_hooks = domain_event_hooks(&other_pool);
        assert!(!Arc::ptr_eq(&hooks.notify, &other_hooks.notify));
        for _ in 0..100 {
            sqlx::query("SELECT 1").fetch_one(&pool).await.unwrap();
        }
        for _ in 0..20 {
            sqlx::query("INSERT INTO other VALUES ('ordinary write')")
                .execute(&pool)
                .await
                .unwrap();
        }
        assert_eq!(hooks.release_inspections.load(Ordering::Relaxed), 0);

        let mut tx = begin_immediate(&pool).await.unwrap();
        sqlx::query("INSERT INTO domain_event VALUES (1, 'rolled back')")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        no_signal(&hooks.notify).await;
        {
            let mut tx = begin_immediate(&pool).await.unwrap();
            sqlx::query("INSERT INTO domain_event VALUES (1, 'dropped')")
                .execute(&mut *tx)
                .await
                .unwrap();
        }
        no_signal(&hooks.notify).await;
        sqlx::query("INSERT INTO other VALUES ('after rollback')")
            .execute(&pool)
            .await
            .unwrap();
        no_signal(&hooks.notify).await;

        // Enable before commit, and immediately read from another connection
        // on delivery: a durable wake can never precede visibility.
        let notified = hooks.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let mut tx = begin_immediate(&pool).await.unwrap();
        sqlx::query("INSERT INTO domain_event VALUES (1, 'committed')")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), &mut notified)
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM domain_event")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
        no_signal(&other_hooks.notify).await; // W4: no cross-database wake.

        // W2: a prior commit survives a later rollback on a held connection.
        let notified = hooks.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let mut conn = pool.acquire().await.unwrap();
        sqlx::raw_sql("BEGIN IMMEDIATE; INSERT INTO domain_event VALUES (2, 'held'); COMMIT;")
            .execute(&mut *conn)
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM domain_event")
                .fetch_one(&pool)
                .await
                .unwrap(),
            2
        );
        assert!(tokio::time::timeout(Duration::from_secs(1), &mut notified)
            .await
            .is_err());
        sqlx::raw_sql(
            "BEGIN IMMEDIATE; INSERT INTO domain_event VALUES (3, 'rolled back later'); ROLLBACK;",
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        drop(conn);
        tokio::time::timeout(Duration::from_secs(2), &mut notified)
            .await
            .unwrap();

        // W3: savepoint rollback cannot lose an earlier surviving insert.
        let notified = hooks.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let mut tx = begin_immediate(&pool).await.unwrap();
        sqlx::raw_sql(
            "INSERT INTO domain_event VALUES (3, 'survives'); SAVEPOINT s;
            INSERT INTO domain_event VALUES (4, 'nested'); ROLLBACK TO s; RELEASE s;",
        )
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), &mut notified)
            .await
            .unwrap();
        let mut tx = begin_immediate(&pool).await.unwrap();
        {
            let mut child = sqlx::Acquire::begin(&mut *tx).await.unwrap();
            sqlx::query("INSERT INTO domain_event VALUES (4, 'inner commit')")
                .execute(&mut *child)
                .await
                .unwrap();
            child.commit().await.unwrap();
        }
        tx.rollback().await.unwrap();
        no_signal(&hooks.notify).await;

        // A rolled-back savepoint may produce a harmless hint, never an event.
        let mut tx = begin_immediate(&pool).await.unwrap();
        sqlx::raw_sql(
            "SAVEPOINT s; INSERT INTO domain_event VALUES (4, 'gone'); ROLLBACK TO s; RELEASE s;",
        )
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let mut conn = pool.acquire().await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_event")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(count, 3);
        drop(conn);

        // W1/W3 autocommit and failed statement; no stale dirty flag.
        let notified = hooks.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        sqlx::query("INSERT INTO domain_event VALUES (4, 'autocommit')")
            .execute(&pool)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), &mut notified)
            .await
            .unwrap();
        assert!(
            sqlx::query("INSERT INTO domain_event VALUES (4, 'duplicate')")
                .execute(&pool)
                .await
                .is_err()
        );
        no_signal(&hooks.notify).await;

        // N5: poisoning is recovered, not translated by sqlx into ROLLBACK.
        let poisoned = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = hooks.guard();
            panic!("poison test");
        }));
        assert!(poisoned.is_err());
        let notified = hooks.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        sqlx::query("INSERT INTO domain_event VALUES (5, 'after poison')")
            .execute(&pool)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), &mut notified)
            .await
            .unwrap();
        pool.close().await;
        other_pool.close().await;
    }
}
