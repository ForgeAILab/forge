//! Parity coverage for persistence moved from the API route layer.
use db::{create_sqlite_pool, run_migrations, SqliteDb};

#[path = "route_persistence/cleanup.rs"]
mod cleanup;
#[path = "route_persistence/leaf.rs"]
mod leaf;
#[path = "route_persistence/media.rs"]
mod media;

const NOW: &str = "2026-10-01T00:00:00Z";
const LATER: &str = "2026-10-01T00:01:00Z";

async fn database() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    SqliteDb::new(pool)
}

async fn user(db: &SqliteDb, id: &str) {
    sqlx::query("INSERT INTO user (id, email, password_hash, created_at, updated_at) VALUES (?, ?, 'test', ?, ?)")
        .bind(id).bind(format!("{id}@example.test")).bind(NOW).bind(NOW)
        .execute(db.pool()).await.unwrap();
}

async fn project(db: &SqliteDb, id: &str, owner_id: Option<&str>) {
    sqlx::query(
        "INSERT INTO project (id, name, owner_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(id)
    .bind(id)
    .bind(owner_id)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .unwrap();
}

async fn task(db: &SqliteDb, id: &str, project_id: &str, status: &str) {
    sqlx::query("INSERT INTO task (id, project_id, title, task_type, status, created_at, updated_at) VALUES (?, ?, ?, 'task', ?, ?, ?)")
        .bind(id).bind(project_id).bind(id).bind(status).bind(NOW).bind(NOW)
        .execute(db.pool()).await.unwrap();
}
