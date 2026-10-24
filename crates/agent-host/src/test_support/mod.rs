//! File-backed fixtures available only with the explicit `test-support` feature.

mod journal_events;
pub use journal_events::JournalEvents;
mod mapping;
pub use mapping::MappingStorage;

use std::{path::Path, sync::Arc};

use agent_runtime::core::{error::RuntimeError, ids::SessionId};
use db::SqliteDb;
use tempfile::TempDir;

/// Owns the database directory for as long as any fixture/storage clone lives.
#[derive(Clone)]
pub struct FileDb {
    db: Arc<SqliteDb>,
    directory: Arc<TempDir>,
}

impl std::fmt::Debug for FileDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileDb").finish_non_exhaustive()
    }
}

impl FileDb {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let directory = Arc::new(tempfile::tempdir()?);
        let pool = db::create_sqlite_pool(&format!(
            "sqlite://{}",
            directory.path().join("journal.db").display()
        ))
        .await?;
        db::run_migrations(&pool).await?;
        let db = Arc::new(SqliteDb::new(pool));
        let now = db::now_rfc3339();
        for statement in [
            "INSERT INTO agent_identity (id, name, created_at, updated_at)
             VALUES ('journal-agent', 'Journal fixture', ?, ?)",
            "INSERT INTO agent_profile (id, identity_id, backend_kind, executor_type, created_at, updated_at)
             VALUES ('journal-profile', 'journal-agent', 'native', 'embedded', ?, ?)",
            "INSERT INTO agent_context_scope (id, identity_id, scope_type, scope_id, created_at, updated_at)
             VALUES ('journal-scope', 'journal-agent', 'account', 'journal-owner', ?, ?)",
        ] {
            sqlx::query(statement)
                .bind(&now)
                .bind(&now)
                .execute(db.pool())
                .await?;
        }
        Ok(Self { db, directory })
    }

    /// Seed authenticated rows directly for read/corruption and boundary fixtures.
    /// This test-only helper bypasses the apply fence.
    pub async fn seed_journal(
        &self,
        storage: &crate::session_journal::ForgeJournalStorage,
        write: agent_runtime::core::journal::JournalWrite,
    ) -> Result<(), RuntimeError> {
        self.ensure_session(&write.session).await?;
        storage.seed(write).await
    }

    /// Rewrite authenticated pack units with a new generation for cache tests.
    pub async fn reseal_journal_scope(
        &self,
        storage: &crate::session_journal::ForgeJournalStorage,
        domain: &agent_runtime::core::journal::JournalDomain,
        session: &SessionId,
    ) -> Result<(), RuntimeError> {
        storage.reseal_scope(domain, session).await
    }

    pub fn database(&self) -> Arc<SqliteDb> {
        self.db.clone()
    }

    pub fn directory(&self) -> &Path {
        self.directory.path()
    }

    /// A genuinely independent pool over the same file, without copying state.
    pub async fn reopen(&self) -> db::Result<Arc<SqliteDb>> {
        let pool = db::create_sqlite_pool(&format!(
            "sqlite://{}",
            self.directory.path().join("journal.db").display()
        ))
        .await?;
        Ok(Arc::new(SqliteDb::new(pool)))
    }

    /// Creates mappings on demand, never from an upstream-id allowlist. The
    /// transaction also serializes simultaneous first applies in two domains.
    pub async fn ensure_session(&self, runtime: &SessionId) -> Result<String, RuntimeError> {
        let mut tx = db::begin_immediate(self.db.pool())
            .await
            .map_err(|_| RuntimeError::internal("fixture mapping transaction failed"))?;
        let ids: Vec<String> =
            sqlx::query_scalar("SELECT id FROM agent_session WHERE runtime_session_id = ?")
                .bind(runtime.as_str())
                .fetch_all(&mut *tx)
                .await
                .map_err(|_| RuntimeError::internal("fixture mapping lookup failed"))?;
        let id = match ids.as_slice() {
            [id] => id.clone(),
            [] => {
                let id = format!("journal-fixture:{}", runtime.as_str());
                let now = db::now_rfc3339();
                // Suspended fixtures do not consume the scope's active slot.
                sqlx::query(
                    "INSERT INTO agent_session
                     (id, identity_id, profile_id, context_scope_id, backend_kind,
                      runtime_session_id, status, created_at, updated_at)
                     VALUES (?, 'journal-agent', 'journal-profile', 'journal-scope',
                             'native', ?, 'suspended', ?, ?)",
                )
                .bind(&id)
                .bind(runtime.as_str())
                .bind(&now)
                .bind(&now)
                .execute(&mut *tx)
                .await
                .map_err(|_| RuntimeError::internal("fixture mapping insert failed"))?;
                id
            }
            _ => {
                return Err(RuntimeError::internal(
                    "fixture runtime mapping is ambiguous",
                ));
            }
        };
        tx.commit()
            .await
            .map_err(|_| RuntimeError::internal("fixture mapping commit failed"))?;
        Ok(id)
    }
}
