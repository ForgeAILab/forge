use crate::{now_rfc3339, DbError, Result};
use include_dir::{include_dir, Dir};
use sqlx::SqlitePool;
use std::{
    fs,
    path::{Path, PathBuf},
};

// Embed every migration .sql file into the binary at compile time so a released
// binary has no filesystem dependency on the source tree.
// Keep this module's source revisioned when adding a migration: include_dir's
// directory dependency is intentionally compile-time and older Cargo versions
// do not always notice a newly-created file under the directory (or a changed
// migration after the initial build).
// Embedded migration bundle revision: V105.
static MIGRATIONS_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/migrations");

#[derive(Debug, Clone, PartialEq, Eq)]
struct Migration {
    version: i64,
    name: String,
    path: PathBuf,
}

pub async fn run_migrations(pool: &SqlitePool) -> Result<()> {
    ensure_migration_table(pool).await?;

    let mut migrations: Vec<(Migration, String)> = MIGRATIONS_DIR
        .files()
        .filter(|file| file.path().extension().and_then(|ext| ext.to_str()) == Some("sql"))
        .map(|file| {
            let migration = parse_migration_path(file.path().to_path_buf())?;
            let sql = file
                .contents_utf8()
                .ok_or_else(|| DbError::InvalidMigrationFilename {
                    path: file.path().to_path_buf(),
                })?
                .to_string();
            Ok::<_, DbError>((migration, sql))
        })
        .collect::<Result<_>>()?;

    migrations.sort_by_key(|(migration, _)| migration.version);

    for (migration, sql) in migrations {
        if is_applied(pool, migration.version).await? {
            continue;
        }
        apply_migration_sql(pool, &migration, &sql).await?;
    }

    Ok(())
}

pub async fn run_migrations_from(pool: &SqlitePool, migration_dir: impl AsRef<Path>) -> Result<()> {
    ensure_migration_table(pool).await?;

    let migrations = discover_migrations(migration_dir.as_ref())?;
    for migration in migrations {
        if is_applied(pool, migration.version).await? {
            continue;
        }
        apply_migration(pool, &migration).await?;
    }

    Ok(())
}

async fn ensure_migration_table(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS _migration (
            version     INTEGER PRIMARY KEY,
            name        TEXT NOT NULL,
            applied_at  TEXT NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

fn discover_migrations(migration_dir: &Path) -> Result<Vec<Migration>> {
    let entries = fs::read_dir(migration_dir).map_err(|source| DbError::ReadMigrationDir {
        path: migration_dir.to_path_buf(),
        source,
    })?;
    let mut migrations = Vec::new();

    for entry in entries {
        let path = entry
            .map_err(|source| DbError::ReadMigrationDir {
                path: migration_dir.to_path_buf(),
                source,
            })?
            .path();

        if path.extension().and_then(|extension| extension.to_str()) != Some("sql") {
            continue;
        }

        migrations.push(parse_migration_path(path)?);
    }

    migrations.sort_by_key(|migration| migration.version);
    Ok(migrations)
}

fn parse_migration_path(path: PathBuf) -> Result<Migration> {
    let filename = path
        .file_name()
        .and_then(|filename| filename.to_str())
        .ok_or_else(|| DbError::InvalidMigrationFilename { path: path.clone() })?;
    let stem = filename
        .strip_suffix(".sql")
        .ok_or_else(|| DbError::InvalidMigrationFilename { path: path.clone() })?;
    let (version_part, name) = stem
        .split_once("__")
        .ok_or_else(|| DbError::InvalidMigrationFilename { path: path.clone() })?;
    let version = version_part
        .strip_prefix('V')
        .ok_or_else(|| DbError::InvalidMigrationFilename { path: path.clone() })?
        .parse::<i64>()
        .map_err(|source| DbError::InvalidMigrationVersion {
            path: path.clone(),
            source,
        })?;

    Ok(Migration {
        version,
        name: name.to_owned(),
        path,
    })
}

async fn is_applied(pool: &SqlitePool, version: i64) -> Result<bool> {
    let applied = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _migration WHERE version = ?")
        .bind(version)
        .fetch_one(pool)
        .await?;
    Ok(applied > 0)
}

async fn apply_migration(pool: &SqlitePool, migration: &Migration) -> Result<()> {
    let sql = fs::read_to_string(&migration.path).map_err(|source| DbError::ReadMigrationFile {
        path: migration.path.clone(),
        source,
    })?;
    apply_migration_sql(pool, migration, &sql).await
}

async fn apply_migration_sql(pool: &SqlitePool, migration: &Migration, sql: &str) -> Result<()> {
    if migration_requires_direct_connection(sql) {
        let mut connection = pool.acquire().await?;
        sqlx::raw_sql(sql).execute(&mut *connection).await?;
        sqlx::query("INSERT INTO _migration (version, name, applied_at) VALUES (?, ?, ?)")
            .bind(migration.version)
            .bind(&migration.name)
            .bind(now_rfc3339())
            .execute(&mut *connection)
            .await?;
    } else {
        let mut transaction = crate::begin_immediate(pool).await?;

        sqlx::raw_sql(sql).execute(&mut *transaction).await?;
        sqlx::query("INSERT INTO _migration (version, name, applied_at) VALUES (?, ?, ?)")
            .bind(migration.version)
            .bind(&migration.name)
            .bind(now_rfc3339())
            .execute(&mut *transaction)
            .await?;

        transaction.commit().await?;
    }

    Ok(())
}

fn migration_requires_direct_connection(sql: &str) -> bool {
    // SQLite ignores `PRAGMA foreign_keys = ...` inside an open transaction, so
    // migrations that rebuild referenced tables must run directly on a single
    // connection instead of inside the default transaction wrapper.
    sql.to_ascii_lowercase().contains("pragma foreign_keys")
}
