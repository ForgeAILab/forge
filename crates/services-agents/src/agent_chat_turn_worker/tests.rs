//! Shared fixtures and test modules for Agent Chat turns.

use super::*;

mod handoff;
mod prompts;
mod worker;

async fn worker_test_db() -> Arc<SqliteDb> {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("in-memory pool");
    db::run_migrations(&pool).await.expect("migrations apply");
    Arc::new(SqliteDb::new(pool))
}
