//! Lowest-priority legacy conversion, isolated from collection scheduling.
use agent_runtime::core::{error::ErrorKind, ids::SessionId};
use db::SqliteDb;
use forge_agent_host::{
    session_journal::ForgeJournalStorage, session_journal_conversion::ConversionOutcome,
    SqliteProtectedRuntimeStore,
};
use services::{Result, ServiceError};
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle, time::Instant};

pub const CONVERSION_INTERVAL: Duration = Duration::from_secs(60);
pub const CONVERSION_SCHEDULING_BUDGET: Duration = Duration::from_secs(2);
pub const CONVERSION_CANDIDATE_LIMIT: u32 = 8;

#[derive(Default)]
pub struct ConversionCursor {
    after: String,
}

#[derive(Debug, Default)]
pub struct ConversionReport {
    pub examined: usize,
    pub converted: usize,
    pub skipped_active: usize,
    pub skipped_unreadable: usize,
    pub conflicts: usize,
    pub remaining: u64,
    pub budget_exhausted: bool,
}

pub struct SessionJournalConversion {
    db: Arc<SqliteDb>,
    store: Arc<SqliteProtectedRuntimeStore>,
}
impl SessionJournalConversion {
    pub fn new(db: Arc<SqliteDb>, store: Arc<SqliteProtectedRuntimeStore>) -> Self {
        Self { db, store }
    }

    pub fn start(
        self: Arc<Self>,
        workers: &services::worker_runtime::PeriodicWorkers,
        shutdown: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        workers.worker("session-journal-conversion")
            // A single import may exceed this scheduling budget. Do not cancel
            // it: exact-first partial progress is repaired by the runtime.
            .with_stall_budget(CONVERSION_SCHEDULING_BUDGET)
            .start(shutdown, || false, move |worker, mut shutdown| {
                let conversion = self.clone();
                async move {
                    let mut cursor = ConversionCursor::default();
                    let mut tick = tokio::time::interval(CONVERSION_INTERVAL);
                    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    loop {
                        tokio::select! {
                            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow_and_update() { return Ok(()); } }
                            _ = tick.tick() => {
                                if *shutdown.borrow_and_update() { return Ok(()); }
                                if let Err(error) = worker.tick(conversion.convert_once(&mut cursor)).await {
                                    tracing::warn!(worker=worker.name(), %error, "journal conversion pass deferred");
                                }
                            }
                        }
                    }
                }
            })
    }

    pub async fn convert_once(&self, cursor: &mut ConversionCursor) -> Result<ConversionReport> {
        self.convert_for(cursor, CONVERSION_SCHEDULING_BUDGET).await
    }

    #[cfg(feature = "test-support")]
    pub async fn convert_with_budget_for_test(
        &self,
        cursor: &mut ConversionCursor,
        budget: Duration,
    ) -> Result<ConversionReport> {
        self.convert_for(cursor, budget).await
    }

    async fn convert_for(
        &self,
        cursor: &mut ConversionCursor,
        budget: Duration,
    ) -> Result<ConversionReport> {
        let started = Instant::now();
        let deadline = started + budget;
        let mut report = ConversionReport::default();
        let page = self
            .db
            .legacy_journal_conversion_page(&cursor.after, CONVERSION_CANDIDATE_LIMIT)
            .await?;
        if page.is_empty() {
            cursor.after.clear();
        }
        for (owner, runtime) in page {
            if Instant::now() >= deadline {
                report.budget_exhausted = true;
                break;
            }
            let outcome = self
                .store
                .convert_legacy_session(&owner, &SessionId::new(&runtime))
                .await;
            match outcome {
                Ok(ConversionOutcome::Converted) => report.converted += 1,
                Ok(ConversionOutcome::AlreadyNative) => {}
                Ok(ConversionOutcome::SkippedActive) => report.skipped_active += 1,
                Err(error)
                    if error.kind == ErrorKind::Conflict || error.kind == ErrorKind::NotFound =>
                {
                    report.conflicts += 1
                }
                Err(error) if error.kind == ErrorKind::Serialization && !error.retryable => {
                    ForgeJournalStorage::note_unreadable_session(&SessionId::new(runtime));
                    report.skipped_unreadable += 1;
                }
                Err(_) => {
                    return Err(ServiceError::Domain(
                        "session journal conversion deferred".into(),
                    ))
                }
            }
            cursor.after = owner;
            report.examined += 1;
            tokio::task::yield_now().await;
        }
        report.budget_exhausted |= Instant::now() >= deadline;
        report.remaining = self.db.legacy_journal_conversion_remaining().await?;
        tracing::debug!(target: "forge_session_journal", duration_us=started.elapsed().as_micros() as u64,
            examined=report.examined, converted=report.converted, skipped_active=report.skipped_active,
            skipped_unreadable=report.skipped_unreadable, conflicts=report.conflicts, remaining=report.remaining,
            budget_exhausted=report.budget_exhausted, "journal conversion pass");
        Ok(report)
    }
}
