//! Bounded production checking of the shadow, never Task workflow state.
//!
//! A page is read and recomputed on a reader connection. The writer is taken
//! only for a row that differs, one fenced statement per row, so a healthy
//! database never waits on, or holds, the write lock for this check.
use super::*;
use std::time::{Duration, Instant};

/// Rows recomputed per supervised tick.
pub const CONDITION_CHECK_PAGE: usize = 50;
/// Cadence of the steady-state check.
const CHECK_INTERVAL: Duration = Duration::from_secs(120);
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
    pub(crate) next: Option<Instant>,
    pub(crate) status: ConditionCheckStatus,
    pass: ConditionCheckPass,
    /// Whether the recorded mapping revision is stale: unknown until the
    /// first tick reads it, then true until one pass completes.
    backfill: Option<bool>,
    cursor_loaded: bool,
    page_ids: Vec<String>,
}
// Supervised loop cancellation must not disable subsequent checks.
struct CheckGuard(std::sync::Arc<std::sync::Mutex<ConditionCheckState>>);
impl Drop for CheckGuard {
    fn drop(&mut self) {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).in_progress = false;
    }
}
struct Page {
    ids: Vec<String>,
    checked: u64,
    repaired: u64,
    /// The cursor after this page; `None` once the last Task was read.
    next: Option<String>,
}
impl SqliteDb {
    pub async fn begin_condition_sweep(&self, startup: bool) -> Result<()> {
        let unknown = self
            .condition_checks
            .lock()
            .expect("condition checks")
            .backfill
            .is_none();
        if unknown {
            let recorded =
                crate::SystemSettingRepo::get_setting(self, MAPPING_REVISION_KEY).await?;
            let mut state = self.condition_checks.lock().expect("condition checks");
            state.backfill =
                Some(recorded.as_deref() != Some(MAPPING_REVISION.to_string().as_str()));
        }
        if startup {
            // Startup proves the entire current set, even when a previous
            // process persisted a partial pass's cursor.
            let mut state = self.condition_checks.lock().expect("condition checks");
            state.after = None;
            state.cursor_loaded = true;
            state.pass = ConditionCheckPass::default();
        }
        Ok(())
    }
    pub async fn persist_condition_cursor(&self) -> Result<()> {
        let cursor = self
            .condition_checks
            .lock()
            .expect("condition checks")
            .after
            .clone();
        sqlx::query(
            "UPDATE task_schedule_sweep SET cursor=? WHERE singleton=1 AND cursor IS NOT ?",
        )
        .bind(&cursor)
        .bind(&cursor)
        .execute(self.pool())
        .await?;
        Ok(())
    }
    pub fn condition_check_page_ids(&self) -> (Vec<String>, Option<String>) {
        let state = self.condition_checks.lock().expect("condition checks");
        (state.page_ids.clone(), state.after.clone())
    }
    pub fn count_schedule_repairs(&self, repairs: u64) {
        let mut state = self.condition_checks.lock().expect("condition checks");
        state.status.repaired += repairs;
        if let Some(pass) = state.status.last_pass.as_mut() {
            pass.repaired += repairs;
        }
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
        let loaded = self
            .condition_checks
            .lock()
            .expect("condition checks")
            .cursor_loaded;
        if !loaded {
            let cursor: Option<String> =
                sqlx::query_scalar("SELECT cursor FROM task_schedule_sweep WHERE singleton=1")
                    .fetch_one(self.pool())
                    .await?;
            let mut state = self.condition_checks.lock().expect("condition checks");
            state.after = cursor;
            state.cursor_loaded = true;
        }
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
            state.page_ids = page.ids.clone();
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
    /// The supervised tick. It never blocks startup: the first call only
    /// reads the recorded mapping revision. While that revision is stale the
    /// backfill is re-run in slices on every tick; afterwards one page is
    /// checked per [`CHECK_INTERVAL`].
    pub async fn check_task_conditions_if_due(&self) -> Result<()> {
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
        if backfilling {
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
            return Ok(());
        }
        {
            let mut state = self.condition_checks.lock().expect("condition checks");
            let now = Instant::now();
            if state.next.is_some_and(|next| next > now) {
                return Ok(());
            }
            state.next = Some(now + CHECK_INTERVAL);
        }
        self.check_task_conditions(CONDITION_CHECK_PAGE)
            .await
            .map(|_| ())
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
                ids: Vec::new(),
                checked: 0,
                repaired: 0,
                next: after.map(str::to_owned),
            });
        }
        // One read snapshot for the page, so a row and its facts agree.
        let mut reader = self.pool().begin().await?;
        let rows = sqlx::query(&format!(
            "SELECT {LEGACY_SELECT},id,version FROM task WHERE ?1 IS NULL OR id>?1 ORDER BY id LIMIT ?2"
        ))
        .bind(after)
        .bind(limit as i64)
        .fetch_all(&mut *reader)
        .await?;
        let mut stale = Vec::new();
        for row in &rows {
            let id: String = row.try_get(6)?;
            let expected = async {
                let input = LegacyConditionInput::from_row(row)?;
                let facts = ConditionFacts::load(&mut reader, &id).await?;
                Ok::<_, DbError>(encode(&facts.apply(map_legacy_condition(&input))))
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
            ids: rows
                .iter()
                .map(|r| r.try_get(6))
                .collect::<std::result::Result<Vec<_>, _>>()?,
            checked: rows.len() as u64,
            repaired,
            next: (rows.len() == limit)
                .then(|| rows.last().map(|row| row.try_get(6)))
                .flatten()
                .transpose()?,
        })
    }
}
