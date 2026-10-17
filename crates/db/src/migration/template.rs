//! Test-only snapshots. Callers must finish database setup before using the pool.
//! Production and migration-fixture directories always use the replay runner.

use super::{run_migrations_replay, MIGRATION_TABLE_SQL};
use crate::{DbError, Result};
use sqlx::{sqlite::SqliteOwnedBuf, Connection, SqliteConnection, SqlitePool};
use std::{io::Write, path::Path};
use tokio::sync::OnceCell;

/// Persistent header settings a copy would overwrite: page size, auto-vacuum
/// mode, user version, application id and encoding.
type Settings = (i64, i64, i64, i64, String);

struct Template {
    bytes: Vec<u8>,
    /// What `create_sqlite_pool` gives a fresh database. Only a target with the
    /// same settings may take the copy; anything else keeps replay.
    settings: Settings,
}

// A byte image outlives the runtime that built it. In particular, do not cache a
// pool: each #[tokio::test] can have its own runtime, which is dropped afterwards.
static TEMPLATE: OnceCell<Template> = OnceCell::const_new();

async fn build_template() -> Result<Template> {
    let pool = crate::create_sqlite_pool("sqlite::memory:").await?;
    let result = async {
        let mut connection = pool.acquire().await?;
        let settings = settings(&mut connection).await?;
        drop(connection);
        run_migrations_replay(&pool).await?;
        let mut connection = pool.acquire().await?;
        Ok(Template {
            bytes: connection.serialize(None).await?.to_vec(),
            settings,
        })
    }
    .await;
    pool.close().await;
    result
}

async fn settings(connection: &mut SqliteConnection) -> Result<Settings> {
    Ok(sqlx::query_as(
        "SELECT page_size, auto_vacuum, user_version, application_id, encoding
         FROM pragma_page_size, pragma_auto_vacuum, pragma_user_version,
              pragma_application_id, pragma_encoding",
    )
    .fetch_one(&mut *connection)
    .await?)
}

async fn is_fresh(connection: &mut SqliteConnection) -> Result<bool> {
    let schema: Vec<(String, Option<String>)> =
        sqlx::query_as("SELECT name, sql FROM main.sqlite_master ORDER BY name")
            .fetch_all(&mut *connection)
            .await?;
    if !schema.is_empty() {
        // An empty, ordinary tracking table is also safe. Any other object,
        // including an altered tracking table, must retain replay semantics.
        let expected_sql = MIGRATION_TABLE_SQL.trim().replacen(" IF NOT EXISTS", "", 1);
        if schema.len() != 1
            || schema[0].0 != "_migration"
            || schema[0].1.as_deref() != Some(expected_sql.as_str())
        {
            return Ok(false);
        }
        let applied: i64 = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM _migration)")
            .fetch_one(&mut *connection)
            .await?;
        if applied != 0 {
            return Ok(false);
        }
    }
    let temporary_objects: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM temp.sqlite_master")
        .fetch_one(&mut *connection)
        .await?;
    // A query-only connection must keep its replay error, and the access probe
    // in `try_restore` relies on an untouched user version.
    let (user_version, query_only): (i64, i64) = sqlx::query_as(
        "SELECT user_version, query_only FROM pragma_user_version, pragma_query_only",
    )
    .fetch_one(&mut *connection)
    .await?;
    Ok(temporary_objects == 0 && user_version == 0 && query_only == 0)
}

pub(super) async fn try_restore(pool: &SqlitePool) -> Result<bool> {
    let mut connection = pool.acquire().await?;
    if !is_fresh(&mut connection).await? {
        return Ok(false);
    }
    let databases: Vec<(i64, String, String)> = sqlx::query_as("PRAGMA database_list")
        .fetch_all(&mut *connection)
        .await?;
    if databases
        .iter()
        .any(|(_, name, _)| name != "main" && name != "temp")
    {
        return Ok(false);
    }
    let filename = &databases
        .iter()
        .find(|(_, name, _)| name == "main")
        .expect("SQLite always has a main database")
        .2;
    if filename.is_empty() {
        // deserialize replaces one connection's database with private memory.
        // Never detach a shared, named database or a multi-connection pool.
        let options = pool.connect_options();
        let name = options.get_filename().to_string_lossy();
        if pool.options().get_max_connections() != 1
            || !(name == ":memory:" || name.starts_with("file:sqlx-in-memory-"))
        {
            return Ok(false);
        }
    } else {
        // The backup connection must not bypass a read-only/query-only sqlx
        // connection. is_fresh checked that user_version is already zero, so
        // this rolled-back header write checks access without changing settings
        // or paying for an extra fsync. BEGIN IMMEDIATE alone also succeeds on
        // read-only WAL databases.
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let access = sqlx::query("PRAGMA user_version = 0")
            .execute(&mut *transaction)
            .await;
        let rollback = transaction.rollback().await;
        access?;
        rollback?;
    }
    let template = TEMPLATE.get_or_try_init(build_template).await?;
    // Initializing the shared snapshot can yield while another connection
    // finishes setting up a file pool. Never overwrite that database. Custom
    // persistent settings also keep replay, so the copy never changes them.
    if !is_fresh(&mut connection).await? || settings(&mut connection).await? != template.settings {
        return Ok(false);
    }
    restore(&mut connection, Path::new(filename), &template.bytes).await?;
    Ok(true)
}

async fn restore(connection: &mut SqliteConnection, path: &Path, bytes: &[u8]) -> Result<()> {
    connection.clear_cached_statements().await?;
    if path.as_os_str().is_empty() {
        connection
            .deserialize(None, SqliteOwnedBuf::try_from(bytes)?, false)
            .await?;
    } else {
        // deserialize would disconnect the disk file. Instead use SQLite's
        // backup API on a separate connection; all pool connections keep their
        // file/WAL handles and observe the copied schema. No raw-handle/unsafe
        // access is needed, and rusqlite 0.32 shares sqlx's libsqlite3-sys 0.30.
        let path = path.to_owned();
        let bytes = bytes.to_vec();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut snapshot = tempfile::Builder::new()
                .prefix(&format!("forge-test-template-{}-", std::process::id()))
                .tempfile_in(std::env::temp_dir())
                .map_err(sqlx::Error::Io)?;
            snapshot.write_all(&bytes).map_err(sqlx::Error::Io)?;
            let source = rusqlite::Connection::open_with_flags(
                snapshot.path(),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .map_err(template_error)?;
            let mut target = rusqlite::Connection::open_with_flags(
                path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            )
            .map_err(template_error)?;
            target
                .busy_timeout(std::time::Duration::from_secs(30))
                .map_err(template_error)?;
            let backup =
                rusqlite::backup::Backup::new(&source, &mut target).map_err(template_error)?;
            match backup.step(-1).map_err(template_error)? {
                rusqlite::backup::StepResult::Done => Ok(()),
                result => Err(DbError::TestTemplate(format!(
                    "SQLite backup did not complete: {result:?}"
                ))),
            }
            // The snapshot file is removed by NamedTempFile, even on errors.
        })
        .await
        .map_err(template_error)??;
    }
    Ok(())
}

fn template_error(error: impl std::fmt::Display) -> DbError {
    DbError::TestTemplate(error.to_string())
}

#[cfg(test)]
mod tests;
