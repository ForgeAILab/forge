//! Bounded production checking of the shadow, never Task workflow state.
//!
//! A page is read and recomputed on a reader connection. The writer is taken
//! only for a row that differs, one fenced statement per row, so a healthy
//! database never waits on, or holds, the write lock for this check.
use super::*;
use std::time::{Duration, Instant};

/// Rows recomputed per page.
pub const CONDITION_CHECK_PAGE: usize = 50;
/// Work one tick may spend re-running the backfill after a mapping change.
const BACKFILL_SLICE: Duration = Duration::from_millis(250);

/// One walk over every Task, first id to last.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConditionCheckPass {
    pub checked: u64,
    pub repaired: u64,
    pub completed_at: String,
}
#[derive(Debug, Clone, Default)]
pub struct ConditionCheckStatus {
    pub checked: u64,
    pub repaired: u64,
    pub ticks: u64,
    pub last_at: Option<String>,
    /// The last completed pass. Operator status reports it only when it
    /// repaired a row; a clean pass is a log line.
    pub last_pass: Option<ConditionCheckPass>,
}
#[derive(Debug, Default)]
pub struct ConditionCheckState {
    pub(crate) after: Option<String>,
    pub(crate) in_progress: bool,
    pub(crate) status: ConditionCheckStatus,
    pass: ConditionCheckPass,
    /// Whether the recorded mapping revision is stale: unknown until the
    /// first tick reads it, then true until one pass completes.
    backfill: Option<bool>,
}
// Supervised loop cancellation must not disable subsequent checks.
struct CheckGuard(std::sync::Arc<std::sync::Mutex<ConditionCheckState>>);
impl Drop for CheckGuard {
    fn drop(&mut self) {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).in_progress = false;
    }
}
struct Page {
    checked: u64,
    repaired: u64,
    /// The cursor after this page; `None` once the last Task was read.
    next: Option<String>,
}
impl SqliteDb {
    /// One lap of the scheduler sweep ended: its condition checks and the
    /// scheduler repairs it counted become the reported pass. A lap that
    /// repaired nothing is a log line.
    pub fn complete_condition_pass(&self, schedule_repairs: u64) {
        let mut state = self.condition_checks.lock().expect("condition checks");
        let mut pass = std::mem::take(&mut state.pass);
        pass.repaired += schedule_repairs;
        pass.completed_at = crate::now_rfc3339();
        state.status.repaired += schedule_repairs;
        // A lap is one run of the check, also when it found no open Task.
        state.status.ticks += 1;
        state.status.last_at = Some(pass.completed_at.clone());
        tracing::info!(
            checked = pass.checked,
            repaired = pass.repaired,
            "Task condition and scheduler invariant pass completed"
        );
        state.status.last_pass = Some(pass);
    }
    /// Recompute and repair exactly these Tasks: the page the scheduler sweep
    /// is on, or one Task whose stored condition could not be decoded.
    pub async fn check_task_conditions_of(&self, ids: &[String]) -> Result<u64> {
        if ids.is_empty() {
            return Ok(0);
        }
        let ids = serde_json::to_string(ids).map_err(|e| DbError::Check(e.to_string()))?;
        let page = self
            .check_condition_rows(
                &format!("SELECT {LEGACY_SELECT},id,version FROM task WHERE id IN (SELECT value FROM json_each(?1)) AND ?2 ORDER BY id"),
                Some(&ids),
                1,
            )
            .await?;
        let mut state = self.condition_checks.lock().expect("condition checks");
        state.status.checked += page.checked;
        state.status.repaired += page.repaired;
        state.status.ticks += 1;
        state.status.last_at = Some(crate::now_rfc3339());
        state.pass.checked += page.checked;
        state.pass.repaired += page.repaired;
        drop(state);
        if page.repaired != 0 {
            tracing::warn!(
                checked = page.checked,
                repaired = page.repaired,
                "Task condition invariant check repaired rows"
            );
        }
        Ok(page.repaired)
    }
    pub fn condition_check_status(&self) -> ConditionCheckStatus {
        self.condition_checks
            .lock()
            .expect("condition checks")
            .status
            .clone()
    }
    /// One indexed keyset page of at most [`CONDITION_CHECK_PAGE`] rows. No
    /// full-table fetch, no workflow write, no event or wake. A failed page
    /// keeps its cursor for a later supervised tick; a row that cannot be
    /// recomputed is logged and passed over.
    pub async fn check_task_conditions(&self, budget: usize) -> Result<ConditionCheckStatus> {
        let (after, limit) = {
            let mut state = self.condition_checks.lock().expect("condition checks");
            if state.in_progress {
                return Ok(state.status.clone());
            }
            state.in_progress = true;
            (state.after.clone(), budget.min(CONDITION_CHECK_PAGE))
        };
        let _running = CheckGuard(self.condition_checks.clone());
        let page = self.check_condition_page(after.as_deref(), limit).await?;
        let (backfilling, completed) = {
            let mut state = self.condition_checks.lock().expect("condition checks");
            state.in_progress = false;
            state.status.checked += page.checked;
            state.status.repaired += page.repaired;
            state.status.ticks += 1;
            state.status.last_at = Some(crate::now_rfc3339());
            state.pass.checked += page.checked;
            state.pass.repaired += page.repaired;
            state.after = page.next.clone();
            let backfilling = state.backfill == Some(true);
            let completed = (limit != 0 && page.next.is_none()).then(|| {
                let mut pass = std::mem::take(&mut state.pass);
                pass.completed_at = crate::now_rfc3339();
                if !backfilling {
                    // Rewriting after a mapping change is the expected
                    // result of an upgrade, not a missed producer.
                    state.status.last_pass = Some(pass.clone());
                }
                pass
            });
            (backfilling, completed)
        };
        if page.repaired != 0 && !backfilling {
            tracing::warn!(
                checked = page.checked,
                repaired = page.repaired,
                "Task condition invariant check repaired rows"
            );
        }
        if let Some(pass) = completed {
            tracing::info!(
                checked = pass.checked,
                repaired = pass.repaired,
                backfill = backfilling,
                "Task condition invariant pass completed"
            );
            if backfilling {
                crate::SystemSettingRepo::set_setting(
                    self,
                    MAPPING_REVISION_KEY,
                    &MAPPING_REVISION.to_string(),
                    &crate::now_rfc3339(),
                )
                .await?;
                self.condition_checks
                    .lock()
                    .expect("condition checks")
                    .backfill = Some(false);
            }
        }
        Ok(self.condition_check_status())
    }
    /// Re-run the backfill in slices while the recorded mapping revision is
    /// stale. It never blocks startup: the first call only reads the recorded
    /// revision, and every later call is one bounded slice until the pass
    /// that covers every Task (settled ones included) completes. The steady
    /// check belongs to the scheduler sweep. Returns whether work remains.
    pub async fn backfill_task_conditions_if_stale(&self) -> Result<bool> {
        let known = self
            .condition_checks
            .lock()
            .expect("condition checks")
            .backfill;
        let backfilling = match known {
            Some(backfilling) => backfilling,
            None => {
                let recorded =
                    crate::SystemSettingRepo::get_setting(self, MAPPING_REVISION_KEY).await?;
                let stale = recorded.as_deref() != Some(MAPPING_REVISION.to_string().as_str());
                let mut state = self.condition_checks.lock().expect("condition checks");
                state.backfill = Some(stale);
                if stale {
                    // The pass that clears the marker must cover every row.
                    state.after = None;
                    state.pass = ConditionCheckPass::default();
                }
                stale
            }
        };
        if !backfilling {
            return Ok(false);
        }
        let started = Instant::now();
        let mut ticks = self.condition_check_status().ticks;
        while started.elapsed() < BACKFILL_SLICE {
            let status = self.check_task_conditions(CONDITION_CHECK_PAGE).await?;
            let state = self.condition_checks.lock().expect("condition checks");
            // Done, or another caller holds the check: yield the tick.
            if state.backfill != Some(true) || status.ticks == ticks {
                break;
            }
            ticks = status.ticks;
        }
        Ok(self
            .condition_checks
            .lock()
            .expect("condition checks")
            .backfill
            == Some(true))
    }
    /// One fenced statement: a row whose version or stored condition changed
    /// since the check read it is not written.
    pub(super) async fn repair_condition(
        &self,
        task_id: &str,
        read_version: i64,
        read_condition: &[u8],
        expected: &str,
    ) -> Result<u64> {
        Ok(sqlx::query("UPDATE task SET condition_json=?1 WHERE id=?2 AND version=?3 AND CAST(condition_json AS BLOB)=?4")
            .bind(expected)
            .bind(task_id)
            .bind(read_version)
            .bind(read_condition)
            .execute(self.pool())
            .await?
            .rows_affected())
    }
    async fn check_condition_page(&self, after: Option<&str>, limit: usize) -> Result<Page> {
        if limit == 0 {
            return Ok(Page {
                checked: 0,
                repaired: 0,
                next: after.map(str::to_owned),
            });
        }
        let mut page = self
            .check_condition_rows(
                &format!("SELECT {LEGACY_SELECT},id,version FROM task WHERE ?1 IS NULL OR id>?1 ORDER BY id LIMIT ?2"),
                after,
                limit as i64,
            )
            .await?;
        if page.checked as usize != limit {
            page.next = None;
        }
        Ok(page)
    }
    /// One read snapshot for the rows, so a row and its facts agree. `next`
    /// is the last id read.
    async fn check_condition_rows(
        &self,
        select: &str,
        first: Option<&str>,
        second: i64,
    ) -> Result<Page> {
        let mut reader = self.pool().begin().await?;
        let rows = sqlx::query(select)
            .bind(first)
            .bind(second)
            .fetch_all(&mut *reader)
            .await?;
        let mut stale = Vec::new();
        for row in &rows {
            let id: String = row.try_get(6)?;
            let expected = async {
                let input = LegacyConditionInput::from_row(row)?;
                let facts = ConditionFacts::load(&mut reader, &id).await?;
                Ok::<_, DbError>(encode(&facts.condition(&input)))
            }
            .await;
            match expected {
                Ok(expected) => {
                    let stored: Vec<u8> = row.try_get(5)?;
                    if stored != expected.as_bytes() {
                        stale.push((id, row.try_get::<i64, _>(7)?, stored, expected));
                    }
                }
                Err(error) => {
                    tracing::warn!(task_id = %id, %error, "Task condition could not be recomputed; skipped")
                }
            }
        }
        reader.rollback().await?;
        let mut repaired = 0;
        for (id, version, stored, expected) in stale {
            // A row written since is left to its own producer and the next pass.
            repaired += self
                .repair_condition(&id, version, &stored, &expected)
                .await?;
        }
        Ok(Page {
            checked: rows.len() as u64,
            repaired,
            next: rows.last().map(|row| row.try_get(6)).transpose()?,
        })
    }
}
