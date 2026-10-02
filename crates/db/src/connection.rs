use crate::{DbError, Result, SqliteStorageStatus};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Connection, Sqlite, SqliteConnection, SqlitePool, Transaction,
};
use std::{
    collections::HashSet,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::Duration,
};
use tokio::sync::Notify;

pub(crate) fn domain_event_notify() -> Arc<Notify> {
    static NOTIFY: OnceLock<Arc<Notify>> = OnceLock::new();
    Arc::clone(NOTIFY.get_or_init(|| Arc::new(Notify::new())))
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
    let committed = Arc::new(Mutex::new(HashSet::<usize>::new()));
    let on_connect = Arc::clone(&committed);
    let pool = SqlitePoolOptions::new()
        .max_connections(max_connections)
        // A release must pass through the notification hook rather than the
        // lifetime-expiry close path. Idle connections can still be recycled.
        .max_lifetime(None)
        .acquire_timeout(Duration::from_secs(30))
        .after_connect(move |connection, _metadata| {
            let committed = Arc::clone(&on_connect);
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
                committed.lock().expect("event hook lock").remove(&key);
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
                        committed.lock().expect("event hook lock").insert(key);
                    }
                    true
                });
                Ok(())
            })
        })
        .after_release(move |connection, _metadata| {
            let committed = Arc::clone(&committed);
            Box::pin(async move {
                // Flush an aborted transaction's queued rollback before reading
                // its marker. Ordinary rollbacks never set a commit marker.
                connection.ping().await?;
                let key = connection.lock_handle().await?.as_raw_handle().as_ptr() as usize;
                if committed.lock().expect("event hook lock").remove(&key) {
                    domain_event_notify().notify_waiters();
                }
                Ok(true)
            })
        })
        .connect_with(options)
        .await?;

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
