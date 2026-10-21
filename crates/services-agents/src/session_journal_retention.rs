//! Host lifecycle scheduling; the shared runtime engines own all liveness.
use agent_runtime::core::{error::ErrorKind, ids::SessionId, journal::*};
use db::{AgentContextScopeRepo, AgentSessionRepo, SqliteDb};
use forge_agent_host::session_journal::ForgeJournalStorage;
use forge_agent_host::{
    session_journals::{RetirementProgress, SessionJournals},
    SqliteProtectedRuntimeStore,
};
use services::{Result, ServiceError};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle, time::Instant};

pub const RETENTION_INTERVAL: Duration = Duration::from_secs(60);
pub const RETENTION_SCHEDULING_BUDGET: Duration = Duration::from_secs(10);
pub const MEMBERSHIP_RELEASE_LIMIT: u64 = 20_000;

#[derive(Default)]
enum Phase {
    #[default]
    Retire,
    Protected,
    Ordinary,
    Drain,
}

/// A cursor is local scheduling state only. Restart starts from the durable
/// dirty/orphan indexes, tombstones and revision-bound retirement grants.
#[derive(Default)]
pub struct RetentionCursor {
    phase: Phase,
    retire_after: String,
    cleanup_after: i64,
    reclaimed_objects: u64,
    reclaimed_bytes: u64,
}

#[derive(Debug, Default)]
pub struct RetentionReport {
    pub retired_domains: usize,
    pub conflicts: usize,
    pub deferred_domains: usize,
    pub unreadable_sessions: usize,
    pub preserved_sessions: usize,
    pub collection_calls: usize,
    pub reclaimed_objects: u64,
    pub reclaimed_bytes: u64,
    pub deleted_packs: u64,
    pub deleted_canonical_bytes: u64,
    pub budget_exhausted: bool,
}

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
    pub deferred_domains: usize,
    pub unreadable_sessions: usize,
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

    /// One bounded policy page, also useful for direct recovery operations.
    pub async fn retire_page(&self, after: &str) -> Result<RetirementPage> {
        self.retire_page_until(after, None, &mut HashSet::new())
            .await
    }

    async fn retire_page_until(
        &self,
        after: &str,
        deadline: Option<Instant>,
        unreadable: &mut HashSet<SessionId>,
    ) -> Result<RetirementPage> {
        let ids = self.db.native_journal_retirement_page(after, 128).await?;
        let mut report = RetirementPage::default();
        if !ids.is_empty() {
            report.after = Some(after.to_owned());
        }
        for id in ids {
            if deadline.is_some_and(|at| Instant::now() >= at) {
                break;
            }
            report.examined += 1;
            report.after = Some(id.clone());
            if !self.should_retire(&id).await? {
                continue;
            }
            let Some(row) = self.db.get_agent_session(&id).await? else {
                continue;
            };
            let Some(runtime) = row.runtime_session_id else {
                continue;
            };
            let session = SessionId::new(&runtime);
            // Re-authenticate before controls even if this id was previously
            // unreadable: deletion may have created a new owner for the same
            // runtime id. Bad records still stop before either domain retires.
            report.selected += 1;
            let RetirementProgress {
                retired_domains,
                conflicts,
                unreadable_session,
                deferred_domains,
            } = self
                .journals
                .retire_published(&runtime)
                .await
                .map_err(|_| ServiceError::Domain("session journal retirement deferred".into()))?;
            report.retired_domains += retired_domains;
            report.conflicts += conflicts;
            report.deferred_domains += deferred_domains;
            if unreadable_session {
                ForgeJournalStorage::note_unreadable_session(&session);
                if unreadable.insert(session) {
                    report.unreadable_sessions += 1;
                }
            }
        }
        Ok(report)
    }
}

impl SessionJournalRetention {
    /// One deletion transaction; the worker budgets repeated batches.
    pub async fn drain_deleted_packs(&self, after_pack_id: i64) -> Result<db::JournalCleanupBatch> {
        Ok(self.db.drain_session_journal_orphans(after_pack_id).await?)
    }
}

impl SessionJournalRetention {
    pub fn start(
        self: Arc<Self>,
        workers: &services::worker_runtime::PeriodicWorkers,
        shutdown: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        workers.worker("session-journal-retention")
            // Check the scheduling budget between awaited controls/batches;
            // never cancel a control just because one call exceeds the budget.
            .with_stall_budget(RETENTION_SCHEDULING_BUDGET)
            .start(shutdown, || false, move |worker, mut shutdown| {
                let retention = self.clone();
                async move {
                    let mut cursor = RetentionCursor::default();
                    let mut tick = tokio::time::interval(RETENTION_INTERVAL);
                    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    tick.tick().await; // consume the initial tick; scan immediately below
                    let notify = retention.db.session_journal_cleanup_notify();
                    loop {
                        if *shutdown.borrow_and_update() { return Ok(()); }
                        // Register before scanning, so a post-commit nudge racing
                        // the pass is retained. Notifications never replace reads.
                        let wake = notify.notified();
                        tokio::pin!(wake);
                        wake.as_mut().enable();
                        if let Err(error) = worker.tick(retention.maintain_once(&mut cursor)).await {
                            tracing::warn!(worker=worker.name(), %error, "journal retention pass deferred");
                        }
                        tokio::select! {
                            changed = shutdown.changed() => {
                                if changed.is_err() || *shutdown.borrow_and_update() { return Ok(()); }
                            }
                            _ = tick.tick() => {}
                            _ = &mut wake => {}
                        }
                    }
                }
            })
    }

    pub async fn maintain_once(&self, cursor: &mut RetentionCursor) -> Result<RetentionReport> {
        self.maintain_for(cursor, RETENTION_SCHEDULING_BUDGET).await
    }

    #[cfg(feature = "test-support")]
    pub async fn maintain_with_budget_for_test(
        &self,
        cursor: &mut RetentionCursor,
        budget: Duration,
    ) -> Result<RetentionReport> {
        self.maintain_for(cursor, budget).await
    }

    async fn maintain_for(
        &self,
        cursor: &mut RetentionCursor,
        budget: Duration,
    ) -> Result<RetentionReport> {
        let started = Instant::now();
        let deadline = started + budget;
        let mut report = RetentionReport::default();
        let mut unreadable = HashSet::new();
        loop {
            if Instant::now() >= deadline {
                report.budget_exhausted = true;
                break;
            }
            match cursor.phase {
                Phase::Retire => {
                    let page = self
                        .retire_page_until(&cursor.retire_after, Some(deadline), &mut unreadable)
                        .await?;
                    report.retired_domains += page.retired_domains;
                    report.conflicts += page.conflicts;
                    report.deferred_domains += page.deferred_domains;
                    report.unreadable_sessions += page.unreadable_sessions;
                    if let Some(after) = page.after {
                        cursor.retire_after = after;
                    } else {
                        cursor.retire_after.clear();
                        cursor.phase = Phase::Protected;
                    }
                }
                Phase::Protected => {
                    self.collect(
                        self.journals.protected(),
                        JournalRole::Protected,
                        cursor,
                        &mut report,
                        &mut unreadable,
                    )
                    .await?;
                    cursor.phase = Phase::Ordinary;
                }
                Phase::Ordinary => {
                    self.collect(
                        self.journals.ordinary(),
                        JournalRole::Ordinary,
                        cursor,
                        &mut report,
                        &mut unreadable,
                    )
                    .await?;
                    cursor.phase = Phase::Drain;
                }
                Phase::Drain => {
                    let batch = self.drain_deleted_packs(cursor.cleanup_after).await?;
                    report.deleted_packs += batch.deleted_packs;
                    report.deleted_canonical_bytes += batch.deleted_canonical_bytes;
                    if batch.has_more {
                        cursor.cleanup_after = batch.after_pack_id;
                    } else {
                        cursor.cleanup_after = 0;
                        cursor.phase = Phase::Retire;
                        break;
                    }
                }
            }
        }
        report.preserved_sessions = unreadable.len();
        tracing::debug!(target: "forge_session_journal", duration_us=started.elapsed().as_micros() as u64,
            retired_domains=report.retired_domains, conflicts=report.conflicts, deferred_domains=report.deferred_domains,
            unreadable_sessions=report.unreadable_sessions, preserved_sessions=report.preserved_sessions,
            unreadable_observations_total=ForgeJournalStorage::unreadable_session_observations(),
            collection_calls=report.collection_calls, membership_release_limit=MEMBERSHIP_RELEASE_LIMIT,
            reclaimed_objects=report.reclaimed_objects, reclaimed_bytes=report.reclaimed_bytes,
            cumulative_reclaimed_objects=cursor.reclaimed_objects, cumulative_reclaimed_bytes=cursor.reclaimed_bytes,
            deleted_packs=report.deleted_packs, deleted_canonical_bytes=report.deleted_canonical_bytes,
            budget_exhausted=report.budget_exhausted, "journal retention pass");
        Ok(report)
    }

    async fn collect(
        &self,
        engine: JournalEngine<ForgeJournalStorage>,
        role: JournalRole,
        cursor: &mut RetentionCursor,
        report: &mut RetentionReport,
        unreadable: &mut HashSet<SessionId>,
    ) -> Result<()> {
        let request = JournalGcRequest {
            storage_id: engine.storage_id(),
            expected_root_epoch: engine
                .root_epoch()
                .map_err(|_| ServiceError::Domain("session journal epoch unavailable".into()))?,
            max_objects: MEMBERSHIP_RELEASE_LIMIT,
        };
        let started = Instant::now();
        let result = engine.collect(request).await;
        let collect_duration_us = started.elapsed().as_micros() as u64;
        report.collection_calls += 1;
        match result {
            Ok(gc) => {
                for id in &gc.unreadable_sessions {
                    ForgeJournalStorage::note_unreadable_session(id);
                    if unreadable.insert(id.clone()) {
                        report.unreadable_sessions += 1;
                    }
                }
                report.reclaimed_objects += gc.deleted;
                report.reclaimed_bytes += gc.reclaimed_bytes;
                cursor.reclaimed_objects = cursor.reclaimed_objects.saturating_add(gc.deleted);
                cursor.reclaimed_bytes = cursor.reclaimed_bytes.saturating_add(gc.reclaimed_bytes);
                let stats = self
                    .journals
                    .storage()
                    .byte_stats(&JournalDomain {
                        storage_id: engine.storage_id(),
                        role,
                    })
                    .await
                    .map_err(|_| {
                        ServiceError::Domain("session journal byte diagnostic deferred".into())
                    })?;
                tracing::debug!(target: "forge_session_journal", ?role, collect_duration_us, gate_hold_upper_bound_us=collect_duration_us, examined=gc.examined,
                    reclaimed_objects=gc.deleted, reclaimed_bytes=gc.reclaimed_bytes,
                    unreadable_sessions=gc.unreadable_sessions.len(), retained_bytes=stats.retained_bytes,
                    live_bytes=stats.live_bytes, deferred_bytes=stats.deferred_bytes,
                    diagnostic_skipped_sessions=stats.skipped_sessions, "journal collection");
            }
            Err(error) if error.kind == ErrorKind::Conflict => {
                report.conflicts += 1;
                tracing::debug!(target: "forge_session_journal", ?role, collect_duration_us, gate_hold_upper_bound_us=collect_duration_us, "journal collection epoch conflict; retry later");
            }
            Err(_) => {
                return Err(ServiceError::Domain(
                    "session journal collection deferred".into(),
                ))
            }
        }
        Ok(())
    }
}
