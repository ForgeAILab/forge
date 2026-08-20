use crate::Result;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Sqlite, SqlitePool, Transaction,
};
use std::{str::FromStr, time::Duration};

pub async fn create_sqlite_pool(database_url: &str) -> Result<SqlitePool> {
    let max_connections = if database_url.contains(":memory:") {
        1
    } else {
        5
    };
    let options = SqliteConnectOptions::from_str(database_url)?.create_if_missing(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(30))
        .after_connect(|connection, _metadata| {
            Box::pin(async move {
                sqlx::query("PRAGMA foreign_keys = ON")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("PRAGMA journal_mode = WAL")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("PRAGMA busy_timeout = 30000")
                    .execute(&mut *connection)
                    .await?;
                Ok(())
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
