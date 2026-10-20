//! Retention policy and bounded operations for the future task-21 scheduler.
//! No engine construction, legacy import, collection or worker scheduling here.
use db::{AgentContextScopeRepo, AgentSessionRepo, SqliteDb};
use forge_agent_host::{
    session_journals::{RetirementProgress, SessionJournals},
    SqliteProtectedRuntimeStore,
};
use services::{Result, ServiceError};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct SessionJournalRetention {
    db: Arc<SqliteDb>,
    journals: SessionJournals,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct RetirementPage {
    pub after: Option<String>,
    pub examined: usize,
    pub selected: usize,
    pub retired_domains: usize,
    pub conflicts: usize,
}

impl SessionJournalRetention {
    pub fn new(db: Arc<SqliteDb>, store: &SqliteProtectedRuntimeStore) -> Self {
        Self {
            db,
            journals: store.journals().clone(),
        }
    }

    /// The resume query is authoritative per identity, scope AND profile.
    /// Replaced sources always qualify; a reusable Task or other ready row does
    /// not. Suspended non-chat rows are the startup sweep's unresumable sessions.
    pub async fn should_retire(&self, forge_session_id: &str) -> Result<bool> {
        let Some(session) = self.db.get_agent_session(forge_session_id).await? else {
            return Ok(false);
        };
        if session.backend_kind != "native" {
            return Ok(false);
        }
        if session.status == "replaced" || session.replaced_by_session_id.is_some() {
            return Ok(true);
        }
        if session.status != "suspended" {
            return Ok(false);
        }
        let Some(scope) = self.db.get_context_scope(&session.context_scope_id).await? else {
            return Ok(false);
        };
        if scope.scope_type != "agent_chat" {
            return Ok(true);
        }
        Ok(self
            .db
            .dormant_native_chat_session_id(
                &session.identity_id,
                &session.context_scope_id,
                &session.profile_id,
            )
            .await?
            .as_deref()
            != Some(&session.id))
    }

    /// One bounded policy page. Call again with `after`, or start over after a
    /// restart/conflict. No Forge transaction or pooled connection spans an
    /// engine call. Task 21 supplies scheduling and its time budget later.
    pub async fn retire_page(&self, after: &str) -> Result<RetirementPage> {
        let ids = self.db.native_journal_retirement_page(after, 128).await?;
        let mut report = RetirementPage {
            after: ids.last().cloned(),
            examined: ids.len(),
            ..Default::default()
        };
        for id in ids {
            if !self.should_retire(&id).await? {
                continue;
            }
            let Some(row) = self.db.get_agent_session(&id).await? else {
                continue;
            };
            let Some(runtime) = row.runtime_session_id else {
                continue;
            };
            report.selected += 1;
            let RetirementProgress {
                retired_domains,
                conflicts,
            } = self
                .journals
                .retire_published(&runtime)
                .await
                .map_err(|_| ServiceError::Domain("session journal retirement failed".into()))?;
            report.retired_domains += retired_domains;
            report.conflicts += conflicts;
        }
        Ok(report)
    }
}

impl SessionJournalRetention {
    /// One deletion transaction; task 21 will budget/schedule repeated batches.
    pub async fn drain_deleted_packs(&self, after_pack_id: i64) -> Result<db::JournalCleanupBatch> {
        Ok(self.db.drain_session_journal_orphans(after_pack_id).await?)
    }
}
