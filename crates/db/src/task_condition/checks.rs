//! Bounded production checking of the shadow, never Task workflow state.
use super::*;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Default)]
pub struct ConditionCheckStatus {
    pub checked: u64,
    pub repaired: u64,
    pub ticks: u64,
    pub last_at: Option<String>,
}
#[derive(Debug, Default)]
pub struct ConditionCheckState {
    pub(crate) after: Option<String>,
    pub(crate) in_progress: bool,
    pub(crate) next: Option<Instant>,
    pub(crate) status: ConditionCheckStatus,
}
// Supervised loop cancellation must not disable subsequent checks.
struct CheckGuard(std::sync::Arc<std::sync::Mutex<ConditionCheckState>>);
impl Drop for CheckGuard {
    fn drop(&mut self) {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).in_progress = false;
    }
}
impl SqliteDb {
    pub fn condition_check_status(&self) -> ConditionCheckStatus {
        self.condition_checks
            .lock()
            .expect("condition checks")
            .status
            .clone()
    }
    /// One indexed keyset page, capped at eight rows and 25ms of work between
    /// rows. No full-table fetch, no workflow write, no event or wake. A failed
    /// page keeps its cursor for a later supervised tick.
    pub async fn check_task_conditions(&self, budget: usize) -> Result<ConditionCheckStatus> {
        let (after, limit) = {
            let mut state = self.condition_checks.lock().expect("condition checks");
            if state.in_progress {
                return Ok(state.status.clone());
            }
            state.in_progress = true;
            (state.after.clone(), budget.min(8))
        };
        let _running = CheckGuard(self.condition_checks.clone());
        let result = self.check_condition_page(after.as_deref(), limit).await;
        let mut state = self.condition_checks.lock().expect("condition checks");
        state.in_progress = false;
        match result {
            Ok((checked, repaired, next)) => {
                state.after = next;
                state.status.checked += checked;
                state.status.repaired += repaired;
                state.status.ticks += 1;
                state.status.last_at = Some(crate::now_rfc3339());
                tracing::info!(
                    checked,
                    repaired,
                    total_repaired = state.status.repaired,
                    "Task condition invariant check"
                );
                Ok(state.status.clone())
            }
            Err(error) => Err(error),
        }
    }
    pub async fn check_task_conditions_if_due(&self) -> Result<()> {
        {
            let mut state = self.condition_checks.lock().expect("condition checks");
            let now = Instant::now();
            if state.next.is_some_and(|next| next > now) {
                return Ok(());
            }
            state.next = Some(now + Duration::from_secs(60));
        }
        self.check_task_conditions(8).await.map(|_| ())
    }
    async fn check_condition_page(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(u64, u64, Option<String>)> {
        if limit == 0 {
            return Ok((0, 0, after.map(str::to_owned)));
        }
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let rows = if let Some(after) = after {
            sqlx::query(&format!(
                "SELECT {LEGACY_SELECT},id FROM task WHERE id>? ORDER BY id LIMIT ?"
            ))
            .bind(after)
            .bind(limit as i64)
            .fetch_all(&mut *tx)
            .await?
        } else {
            sqlx::query(&format!(
                "SELECT {LEGACY_SELECT},id FROM task ORDER BY id LIMIT ?"
            ))
            .bind(limit as i64)
            .fetch_all(&mut *tx)
            .await?
        };
        let started = Instant::now();
        let mut checked = 0;
        let mut repaired = 0;
        let mut next = None;
        for row in &rows {
            if checked != 0 && started.elapsed() >= Duration::from_millis(25) {
                break;
            }
            let id: String = row.try_get(6)?;
            let input = LegacyConditionInput::from_row(row)?;
            let facts = ConditionFacts::load(&mut tx, &id).await?;
            let encoded = encode(&facts.apply(map_legacy_condition(&input)));
            if row.try_get::<Vec<u8>, _>(5)? != encoded.as_bytes() {
                set_condition(&mut tx, &id, &encoded, Some(facts.version)).await?;
                repaired += 1;
            }
            checked += 1;
            next = Some(id);
        }
        if rows.len() < limit && checked == rows.len() as u64 {
            next = None;
        }
        tx.commit().await?;
        Ok((checked, repaired, next))
    }
}
