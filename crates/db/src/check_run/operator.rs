//! The operator's summary of the check runner: read-only counts and ages.
use crate::{Result, SqliteDb};

/// A count and the age in seconds of the oldest row counted.
pub type CheckBacklogRow = (i64, Option<i64>);

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CheckOperatorSummary {
    pub queued: CheckBacklogRow,
    pub running: CheckBacklogRow,
    pub waiting_for_slot: CheckBacklogRow,
    pub waiting_for_disk: CheckBacklogRow,
    pub reuse_hits_24h: i64,
    pub runs_24h: i64,
}

/// What a hooks step that waits for free disk records as its awaited
/// consumer (`services::workflow::engine::durable::DISK_WAIT`).
pub const DISK_WAIT_SENTINEL: &str = "disk";

impl SqliteDb {
    /// Six indexed reads; nothing is written. Live runs are found through
    /// `check_run_state_lease`, suspended steps through the partial index on
    /// `status = 'suspended'`; the two 24-hour counts scan a day of rows.
    pub async fn check_operator_summary(&self) -> Result<CheckOperatorSummary> {
        let backlog = |filter: &'static str, since: &'static str| async move {
            sqlx::query_as::<_, CheckBacklogRow>(&format!(
                "SELECT COUNT(*), CAST((julianday('now')-julianday(MIN({since})))*86400 AS INTEGER) FROM check_run WHERE {filter}"
            ))
            .fetch_one(self.pool())
            .await
        };
        let queued = backlog(
            "state='queued' AND capacity_wait_since IS NULL",
            "created_at",
        )
        .await?;
        let waiting_for_slot = backlog(
            "state='queued' AND capacity_wait_since IS NOT NULL",
            "capacity_wait_since",
        )
        .await?;
        let running = backlog(
            "state IN ('running','cancelling','cleaning','uncertain')",
            "COALESCE(admitted_at, updated_at)",
        )
        .await?;
        let waiting_for_disk = sqlx::query_as::<_, CheckBacklogRow>(
            "SELECT COUNT(*), CAST((julianday('now')-julianday(MIN(created_at)))*86400 AS INTEGER) FROM task_step WHERE status='suspended' AND awaited_consumer_id=?",
        )
        .bind(DISK_WAIT_SENTINEL)
        .fetch_one(self.pool())
        .await?;
        // A hit is a consumer whose result already existed when it asked.
        let reuse_hits_24h = sqlx::query_scalar(
            "SELECT COUNT(*) FROM check_consumer c JOIN check_result x ON x.id=c.result_id WHERE julianday(c.created_at) >= julianday('now','-1 day') AND julianday(x.created_at) < julianday(c.created_at)",
        )
        .fetch_one(self.pool())
        .await?;
        let runs_24h = sqlx::query_scalar(
            "SELECT COUNT(*) FROM check_run WHERE julianday(created_at) >= julianday('now','-1 day')",
        )
        .fetch_one(self.pool())
        .await?;
        let clamp = |(count, age): CheckBacklogRow| (count, age.map(|age| age.max(0)));
        Ok(CheckOperatorSummary {
            queued: clamp(queued),
            running: clamp(running),
            waiting_for_slot: clamp(waiting_for_slot),
            waiting_for_disk: clamp(waiting_for_disk),
            reuse_hits_24h,
            runs_24h,
        })
    }
}
