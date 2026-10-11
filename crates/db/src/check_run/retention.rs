//! Retention of check history. Terminal runs, their results (with the output
//! tails) and their consumers are deleted once they are older than the
//! configured number of days, in bounded batches.
//!
//! The predicate is deny-by-default. A row is deleted only when nothing that
//! is not itself finished can still read it:
//!
//! - a **consumer** goes only when it is old, finished (cancelled, applied,
//!   or its Task is gone or left the status entry it asked from), holds no
//!   undelivered result, is awaited by no unfinished Task step, has no
//!   unfinished delivery step, belongs to no open Review attempt, and its
//!   run is not still in flight;
//! - a **run** goes (and its results with it, by the foreign key) only when
//!   it is terminal, old, past the owner acknowledgement window, and no
//!   consumer row at all still names it or one of its results. A consumer
//!   this job could not delete therefore keeps its run and result, whatever
//!   the reason, and so does a reusable result that a recent request hit.
//!
//! Every column that points at one of the three tables is listed in
//! [`CHECK_REFERENCES`]. A test enumerates them from the schema: a reference
//! added later (the integration queue will add its own) fails that test
//! until this predicate accounts for it.
use crate::{Result, SqliteDb};

/// `(table, column, referenced table)`: every foreign key and every id
/// column that points at `check_run`, `check_result` or `check_consumer`,
/// and is handled by [`SqliteDb::prune_check_history`].
pub const CHECK_REFERENCES: &[(&str, &str, &str)] = &[
    ("check_consumer", "result_id", "check_result"),
    ("check_consumer", "run_id", "check_run"),
    ("check_result", "identity_key", "check_run"),
    ("check_result", "run_id", "check_run"),
    ("task_step", "awaited_consumer_id", "check_consumer"),
];

/// A dispatched run is acknowledged to its owner for up to 25 hours after it
/// finished (`check_run_unacknowledged`). No run younger than this is deleted,
/// however short the configured retention.
const ACKNOWLEDGE_WINDOW_HOURS: i64 = 26;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CheckHistoryPruned {
    pub consumers: u64,
    pub runs: u64,
}

impl SqliteDb {
    /// Delete at most `batch` finished consumers and at most `batch`
    /// terminal runs older than `cutoff` (RFC 3339). Two statements, each
    /// its own short write; call again while a full batch comes back.
    pub async fn prune_check_history(
        &self,
        cutoff: &str,
        batch: i64,
    ) -> Result<CheckHistoryPruned> {
        let consumers = sqlx::query(
            "DELETE FROM check_consumer WHERE id IN (
                SELECT c.id FROM check_consumer c
                WHERE julianday(c.created_at) < julianday(?1)
                  AND (c.cancelled_at IS NOT NULL OR c.applied_at IS NOT NULL OR c.task_id IS NULL
                       OR NOT EXISTS(SELECT 1 FROM task t WHERE t.id=c.task_id
                                     AND t.status_epoch=c.status_epoch AND t.deleted_at IS NULL))
                  AND NOT (c.result_id IS NOT NULL AND c.delivery_step_id IS NULL AND c.cancelled_at IS NULL)
                  AND NOT EXISTS(SELECT 1 FROM task_step s WHERE s.awaited_consumer_id=c.id
                                 AND s.status IN ('pending','claimed','suspended'))
                  AND NOT EXISTS(SELECT 1 FROM task_step s WHERE s.id=c.delivery_step_id
                                 AND s.status IN ('pending','claimed','suspended'))
                  AND NOT EXISTS(SELECT 1 FROM review v WHERE v.task_id=c.task_id
                                 AND v.status IN ('running','awaiting_human')
                                 AND v.id=substr(c.request_key,-36))
                  AND NOT EXISTS(SELECT 1 FROM check_run r WHERE r.id=c.run_id
                                 AND r.state NOT IN ('succeeded','failed','cancelled'))
                ORDER BY c.created_at, c.id LIMIT ?2)",
        )
        .bind(cutoff)
        .bind(batch)
        .execute(self.pool())
        .await?
        .rows_affected();
        let runs = sqlx::query(
            "DELETE FROM check_run WHERE id IN (
                SELECT r.id FROM check_run r
                WHERE r.state IN ('succeeded','failed','cancelled')
                  AND julianday(COALESCE(r.finished_at, r.updated_at)) < julianday(?1)
                  AND julianday(COALESCE(r.finished_at, r.updated_at)) < julianday('now', ?3)
                  AND NOT EXISTS(SELECT 1 FROM check_consumer c WHERE c.run_id=r.id)
                  AND NOT EXISTS(SELECT 1 FROM check_consumer c JOIN check_result x ON x.id=c.result_id
                                 WHERE x.run_id=r.id)
                ORDER BY COALESCE(r.finished_at, r.updated_at), r.id LIMIT ?2)",
        )
        .bind(cutoff)
        .bind(batch)
        .bind(format!("-{ACKNOWLEDGE_WINDOW_HOURS} hours"))
        .execute(self.pool())
        .await?
        .rows_affected();
        Ok(CheckHistoryPruned { consumers, runs })
    }
}
