//! Retention of check history. Terminal runs, their results (with the output
//! tails) and their consumers are deleted once they are older than the
//! configured number of days, in bounded batches.
//!
//! The predicate is deny-by-default. A row is deleted only when nothing that
//! is not itself finished can still read it:
//!
//! - a **consumer** goes only when it is old, finished (cancelled, applied,
//!   or its Task is gone or left the status entry it asked from), holds no
//!   undelivered result, is awaited by no unfinished Task step (one that is
//!   not `done` or `superseded`), has no unfinished delivery step, belongs to no open Review attempt, and its
//!   run is not still in flight;
//! - a **run** goes (and its results with it, by the foreign key) only when
//!   it is terminal, old, past the owner acknowledgement window, and no
//!   consumer row at all still names it or one of its results. A consumer
//!   this job could not delete therefore keeps its run and result, whatever
//!   the reason, and so does a reusable result that a recent request hit.
//!
//! Every column that points at one of the three tables is listed in
//! [`CHECK_REFERENCES`], and every JSON or free-text column of the tables
//! that consume checks (Task steps, hook checkpoints, Reviews, the
//! integration queue) in [`CHECK_JSON_COLUMNS`] with what it can hold. A test
//! enumerates both from the schema: a reference added later (the integration
//! queue will add its own) fails that test until this predicate accounts for
//! it.
//!
//! References that are not columns of their own:
//!
//! - `task_hook_checkpoint.effects_json` (`$.ci_consumer`): the consumer a
//!   review-entry hook asked with. A consumer named by a checkpoint of an
//!   unfinished step is kept, also while that step is not suspended on it.
//! - an **integration** consumer names its integration attempt as its
//!   request authority, and `integration_attempt.operation_id` and its
//!   check, receipt, effect and observation JSON can name a run or result.
//!   Whatever an attempt that is not `completed`, `cancelled` or
//!   `superseded` names is kept, and so is every integration consumer of a
//!   Task that has such an attempt.
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

/// `(table, column, what it can hold)`: every JSON or free-text column of
/// the tables that consume checks. `Some(..)` names the reference to a check
/// row the column can carry and that [`SqliteDb::prune_check_history`]
/// honours; `None` is a column that carries no id of a check row.
pub const CHECK_JSON_COLUMNS: &[(&str, &str, Option<&str>)] = &[
    ("integration_attempt", "changed_paths_json", None),
    ("integration_attempt", "checks_json", ATTEMPT_EVIDENCE),
    ("integration_attempt", "conflict_paths_json", None),
    ("integration_attempt", "effect_ack_json", ATTEMPT_EVIDENCE),
    (
        "integration_attempt",
        "effect_intent_json",
        ATTEMPT_EVIDENCE,
    ),
    (
        "integration_attempt",
        "effect_receipts_json",
        ATTEMPT_EVIDENCE,
    ),
    ("integration_attempt", "guard_paths_json", None),
    ("integration_attempt", "import_source_json", None),
    ("integration_attempt", "observations_json", ATTEMPT_EVIDENCE),
    (
        "integration_attempt",
        "operation_receipts_json",
        ATTEMPT_EVIDENCE,
    ),
    ("integration_attempt", "owner_fence_json", None),
    ("integration_attempt", "permit_json", None),
    ("integration_attempt", "phase_timings_json", None),
    ("integration_attempt", "repair_paths_json", None),
    ("integration_attempt", "reviewed_paths_json", None),
    // Command facts copied from the result, and the id of the Task step that
    // wrote them; an open Review keeps its consumer through the request key.
    ("review", "step_results_json", None),
    (
        "task_hook_checkpoint",
        "effects_json",
        Some("$.ci_consumer"),
    ),
    ("task_hook_checkpoint", "result_json", None),
    // A delivery step names its consumer, which names the step back in
    // `check_consumer.delivery_step_id`: an unfinished one keeps it.
    (
        "task_step",
        "payload_json",
        Some("check_consumer.delivery_step_id"),
    ),
    ("task_step", "result_json", None),
];
const ATTEMPT_EVIDENCE: Option<&str> = Some("run, result or operation ids, searched as text");
/// The columns of an integration attempt that can name a check row.
const ATTEMPT_TEXT: &str = "(COALESCE(a.checks_json,'')||COALESCE(a.operation_receipts_json,'')||COALESCE(a.effect_ack_json,'')||COALESCE(a.effect_intent_json,'')||COALESCE(a.effect_receipts_json,'')||COALESCE(a.observations_json,''))";

/// An integration attempt in one of these states reads nothing any more.
const FINAL_INTEGRATION_ATTEMPT: &str = "('completed','cancelled','superseded')";
/// A Task step that can still run: also a parked or failed one, which its
/// owner can drive again (`retry_hook`) and which then reads what it recorded.
const FINISHED_STEP: &str = "('done','superseded')";

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
        let consumers = sqlx::query(&format!(
            "DELETE FROM check_consumer WHERE id IN (
                SELECT c.id FROM check_consumer c
                WHERE julianday(c.created_at) < julianday(?1)
                  AND (c.cancelled_at IS NOT NULL OR c.applied_at IS NOT NULL OR c.task_id IS NULL
                       OR NOT EXISTS(SELECT 1 FROM task t WHERE t.id=c.task_id
                                     AND t.status_epoch=c.status_epoch AND t.deleted_at IS NULL))
                  AND NOT (c.result_id IS NOT NULL AND c.delivery_step_id IS NULL AND c.cancelled_at IS NULL AND c.applied_at IS NULL)
                  AND c.id NOT IN (SELECT s.awaited_consumer_id FROM task_step s
                                   WHERE s.status NOT IN {FINISHED_STEP} AND s.awaited_consumer_id IS NOT NULL)
                  AND c.id NOT IN (SELECT json_extract(k.effects_json,'$.ci_consumer')
                                   FROM task_hook_checkpoint k JOIN task_step s ON s.id=k.step_id
                                   WHERE s.status NOT IN {FINISHED_STEP}
                                     AND json_extract(k.effects_json,'$.ci_consumer') IS NOT NULL)
                  AND NOT EXISTS(SELECT 1 FROM task_step s WHERE s.id=c.delivery_step_id
                                 AND s.status NOT IN {FINISHED_STEP})
                  AND NOT EXISTS(SELECT 1 FROM review v WHERE v.task_id=c.task_id
                                 AND v.status IN ('running','awaiting_human')
                                 AND v.id=substr(c.request_key,-36))
                  AND NOT (c.origin='integration' AND EXISTS(
                                 SELECT 1 FROM integration_attempt a
                                 WHERE a.state NOT IN {FINAL_INTEGRATION_ATTEMPT}
                                   AND (a.task_id=c.task_id OR instr(c.request_key,a.id)>0)))
                  AND NOT EXISTS(SELECT 1 FROM check_run r WHERE r.id=c.run_id
                                 AND r.state NOT IN ('succeeded','failed','cancelled'))
                ORDER BY c.created_at, c.id LIMIT ?2)"
        ))
        .bind(cutoff)
        .bind(batch)
        .execute(self.pool())
        .await?
        .rows_affected();
        let runs = sqlx::query(&format!(
            "DELETE FROM check_run WHERE id IN (
                SELECT r.id FROM check_run r
                WHERE r.state IN ('succeeded','failed','cancelled')
                  AND julianday(COALESCE(r.finished_at, r.updated_at)) < julianday(?1)
                  AND julianday(COALESCE(r.finished_at, r.updated_at)) < julianday('now', ?3)
                  AND NOT EXISTS(SELECT 1 FROM check_consumer c WHERE c.run_id=r.id)
                  AND NOT EXISTS(SELECT 1 FROM check_consumer c JOIN check_result x ON x.id=c.result_id
                                 WHERE x.run_id=r.id)
                  AND NOT EXISTS(SELECT 1 FROM integration_attempt a
                                 WHERE a.state NOT IN {FINAL_INTEGRATION_ATTEMPT}
                                   AND (a.operation_id=r.operation_id
                                        OR instr({ATTEMPT_TEXT}, r.id)>0
                                        OR instr({ATTEMPT_TEXT}, r.operation_id)>0
                                        OR EXISTS(SELECT 1 FROM check_result x WHERE x.run_id=r.id
                                                  AND instr({ATTEMPT_TEXT}, x.id)>0)))
                ORDER BY COALESCE(r.finished_at, r.updated_at), r.id LIMIT ?2)"
        ))
        .bind(cutoff)
        .bind(batch)
        .bind(format!("-{ACKNOWLEDGE_WINDOW_HOURS} hours"))
        .execute(self.pool())
        .await?
        .rows_affected();
        Ok(CheckHistoryPruned { consumers, runs })
    }
}
