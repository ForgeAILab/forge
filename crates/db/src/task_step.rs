//! Task cascade outbox; all ordering and ownership decisions are transactional.
use crate::{begin_immediate, now_rfc3339, DbError, Result, SqliteDb};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::{Row, Sqlite, Transaction};

/// A step is valid while its Task is still in the status entry that produced
/// it. Binds: task id, expected status, expected epoch.
pub const STEP_FENCE: &str = "SELECT EXISTS(SELECT 1 FROM task WHERE id=? AND status=? AND status_epoch=? AND deleted_at IS NULL)";
// Both use task_step_settled(status, completed_at); binds: cutoff, now,
// active Task ids (JSON), batch limit.
const PRUNE_SETTLED: &str = "DELETE FROM task_step WHERE id IN (SELECT id FROM task_step WHERE status IN ('done','superseded') AND completed_at<? AND (lease_until IS NULL OR lease_until<?) AND task_id NOT IN (SELECT value FROM json_each(?)) AND NOT EXISTS(SELECT 1 FROM task_remote_operation r WHERE r.step_id=task_step.id AND r.state='running') AND NOT EXISTS(SELECT 1 FROM pending_remote_cancel r WHERE r.step_id=task_step.id) AND NOT EXISTS(SELECT 1 FROM task_step live WHERE live.chain_id=task_step.chain_id AND live.status IN ('pending','claimed')) ORDER BY completed_at LIMIT ?)";
const PRUNE_UNRESOLVED: &str = "DELETE FROM task_step WHERE id IN (SELECT id FROM task_step WHERE status IN ('failed','parked') AND completed_at<? AND (lease_until IS NULL OR lease_until<?) AND task_id NOT IN (SELECT value FROM json_each(?)) AND NOT EXISTS(SELECT 1 FROM task_remote_operation r WHERE r.step_id=task_step.id AND r.state='running') AND NOT EXISTS(SELECT 1 FROM pending_remote_cancel r WHERE r.step_id=task_step.id) AND NOT EXISTS(SELECT 1 FROM task_step live WHERE live.chain_id=task_step.chain_id AND live.status IN ('pending','claimed')) ORDER BY completed_at LIMIT ?)";
// task_step_workflow_ref(workflow_ref_id) answers the reference probe.
const PRUNE_WORKFLOWS: &str = "DELETE FROM task_step_workflow WHERE id IN (SELECT w.id FROM task_step_workflow w WHERE w.last_used_at<? AND NOT EXISTS(SELECT 1 FROM task_step s WHERE s.workflow_ref_id=w.id) LIMIT ?)";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStep {
    pub id: String,
    pub task_id: String,
    pub seq: i64,
    pub kind: String,
    pub payload_json: String,
    pub causation_step_id: Option<String>,
    pub causation_key: String,
    pub chain_id: String,
    pub chain_position: i64,
    pub expected_status: String,
    pub expected_version: i64,
    /// Status epoch of the entry that produced this step; with
    /// `expected_status` it is the step's fence.
    pub expected_epoch: i64,
    pub lane: String,
    pub status: String,
    pub claimed_by: Option<String>,
    pub lease_until: Option<String>,
    pub available_at: String,
    pub attempts: i64,
    pub last_error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
    pub result_json: Option<String>,
    /// False for a queued effect fenced by its own identity: it applies after
    /// a status change and survives a preempting Cancel/Hold.
    pub entry_fenced: bool,
}

#[derive(Debug, Clone)]
pub struct EnqueueTaskStep {
    pub kind: String,
    pub id: String,
    pub task_id: String,
    pub payload_json: String,
    pub causation_step_id: Option<String>,
    pub causation_key: String,
    pub chain_id: String,
    pub chain_position: i64,
    pub expected_status: String,
    pub expected_version: i64,
    /// `None` only when this enqueue shares the producing status CAS's
    /// transaction: the epoch is then read after that update, in that
    /// transaction. Post-commit producers pass their entry's epoch.
    pub expected_epoch: Option<i64>,
    pub lane: String,
    /// Earliest attempt time, including durable retry backoff.
    pub available_at: String,
}

#[async_trait]
pub trait TaskStepRepo: Send + Sync {
    async fn enqueue_step_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        input: &EnqueueTaskStep,
    ) -> Result<String>;
    async fn enqueue_step(&self, input: &EnqueueTaskStep) -> Result<String>;
    async fn task_steps(&self, task_id: &str) -> Result<Vec<TaskStep>>;
    async fn pending_steps(&self, task_id: &str) -> Result<i64>;
    /// The current entry's post-commit hooks row is still pending or
    /// claimed: its entry checks (CI, before-work scripts, dispatch) have not
    /// settled. Matched on status and epoch, as the step fence is.
    async fn entry_hooks_pending(&self, task_id: &str) -> Result<bool>;
    async fn chain_steps(&self, chain_id: &str) -> Result<Vec<TaskStep>>;
    async fn step_workflow(&self, id: &str) -> Result<String>;
    async fn store_step_workflow(&self, definition: &str) -> Result<String>;
    async fn reroute_step(&self, step: &TaskStep, lane: &str) -> Result<()>;
    /// Deletes one bounded batch per retention class: done/superseded rows
    /// completed before `settled_before`, failed/parked rows completed before
    /// `unresolved_before`. Live chains, leases and running Tasks are kept.
    async fn prune_steps(
        &self,
        settled_before: &str,
        unresolved_before: &str,
        limit: i64,
    ) -> Result<u64>;
    async fn step_entry_matches(&self, step: &TaskStep) -> Result<bool>;
    async fn claim_step(
        &self,
        owner: &str,
        task_id: Option<&str>,
        lease_until: &str,
    ) -> Result<Option<TaskStep>>;
    async fn renew_step(&self, id: &str, owner: &str, lease_until: &str) -> Result<bool>;
    async fn finish_step_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        step: &TaskStep,
        status: &str,
        error: Option<&str>,
    ) -> Result<()>;
    async fn release_step(&self, id: &str, owner: &str) -> Result<()>;
    async fn retry_step(&self, step: &TaskStep, error: &str, available_at: &str) -> Result<()>;
    async fn ready_step(&self, id: &str) -> Result<()>;
}

fn row_step(row: sqlx::sqlite::SqliteRow) -> TaskStep {
    TaskStep {
        id: row.get("id"),
        task_id: row.get("task_id"),
        seq: row.get("seq"),
        kind: row.get("kind"),
        payload_json: row.get("payload_json"),
        causation_step_id: row.get("causation_step_id"),
        causation_key: row.get("causation_key"),
        chain_id: row.get("chain_id"),
        chain_position: row.get("chain_position"),
        expected_status: row.get("expected_status"),
        expected_version: row.get("expected_version"),
        expected_epoch: row.get("expected_epoch"),
        lane: row.get("lane"),
        status: row.get("status"),
        claimed_by: row.get("claimed_by"),
        lease_until: row.get("lease_until"),
        available_at: row.get("available_at"),
        attempts: row.get("attempts"),
        last_error: row.get("last_error"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        completed_at: row.get("completed_at"),
        result_json: row.get("result_json"),
        entry_fenced: row.get::<i64, _>("entry_fenced") != 0,
    }
}

#[async_trait]
impl TaskStepRepo for SqliteDb {
    async fn enqueue_step_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        i: &EnqueueTaskStep,
    ) -> Result<String> {
        let now = now_rfc3339();
        sqlx::query("INSERT INTO task_step (id,task_id,seq,kind,payload_json,causation_step_id,causation_key,chain_id,chain_position,expected_status,expected_version,expected_epoch,lane,status,available_at,created_at,updated_at) SELECT ?,?,COALESCE(MAX(seq),0)+1,?,?,?,?,?,?,?,?,COALESCE(?,(SELECT status_epoch FROM task WHERE id=?),0),?,'pending',?,?,? FROM task_step WHERE task_id = ? ON CONFLICT(task_id,causation_key) DO NOTHING")
            .bind(&i.id).bind(&i.task_id).bind(&i.kind).bind(&i.payload_json).bind(&i.causation_step_id).bind(&i.causation_key)
            .bind(&i.chain_id).bind(i.chain_position).bind(&i.expected_status).bind(i.expected_version)
            .bind(i.expected_epoch).bind(&i.task_id).bind(&i.lane).bind(&i.available_at).bind(&now).bind(&now).bind(&i.task_id).execute(&mut **tx).await?;
        let mut superseded = 0;
        if serde_json::from_str::<serde_json::Value>(&i.payload_json)
            .ok()
            .is_some_and(|p| p["preempt"] == true)
        {
            sqlx::query("UPDATE task_step SET priority=1 WHERE id=?")
                .bind(&i.id)
                .execute(&mut **tx)
                .await?;
            superseded = sqlx::query("UPDATE task_step SET status='superseded',last_error='preempted by owner command',completed_at=?,updated_at=? WHERE task_id=? AND seq<(SELECT seq FROM task_step WHERE id=?) AND priority=0 AND integration_started_at IS NULL AND entry_fenced=1 AND status='pending' AND kind IN ('hooks','cascade','command','mutation')")
                .bind(&now).bind(&now).bind(&i.task_id).bind(&i.id).execute(&mut **tx).await?.rows_affected();
        }
        // An enqueued hooks step may become the entry's owner; a preempting
        // command may have superseded the one that was.
        if i.kind == "hooks" || superseded != 0 {
            crate::task_condition::produce(tx, &i.task_id, crate::ConditionChange::Hooks).await?;
        }
        Ok(
            sqlx::query_scalar("SELECT id FROM task_step WHERE task_id = ? AND causation_key = ?")
                .bind(&i.task_id)
                .bind(&i.causation_key)
                .fetch_one(&mut **tx)
                .await?,
        )
    }
    async fn enqueue_step(&self, i: &EnqueueTaskStep) -> Result<String> {
        let mut tx = begin_immediate(self.pool()).await?;
        let id = self.enqueue_step_in_tx(&mut tx, i).await?;
        tx.commit().await?;
        self.domain_event_notify().notify_waiters();
        Ok(id)
    }
    async fn task_steps(&self, task_id: &str) -> Result<Vec<TaskStep>> {
        Ok(
            sqlx::query("SELECT * FROM task_step WHERE task_id = ? ORDER BY seq")
                .bind(task_id)
                .fetch_all(self.pool())
                .await?
                .into_iter()
                .map(row_step)
                .collect(),
        )
    }
    async fn chain_steps(&self, chain_id: &str) -> Result<Vec<TaskStep>> {
        Ok(
            sqlx::query("SELECT * FROM task_step WHERE chain_id=? ORDER BY chain_position,seq")
                .bind(chain_id)
                .fetch_all(self.pool())
                .await?
                .into_iter()
                .map(row_step)
                .collect(),
        )
    }
    async fn store_step_workflow(&self, definition: &str) -> Result<String> {
        sqlx::query("INSERT INTO task_step_workflow(id,definition_json,last_used_at) VALUES (?,?,?) ON CONFLICT(definition_json) DO UPDATE SET last_used_at=excluded.last_used_at").bind(crate::new_uuid_v4()).bind(definition).bind(now_rfc3339()).execute(self.pool()).await?;
        Ok(
            sqlx::query_scalar("SELECT id FROM task_step_workflow WHERE definition_json=?")
                .bind(definition)
                .fetch_one(self.pool())
                .await?,
        )
    }
    async fn step_workflow(&self, id: &str) -> Result<String> {
        Ok(
            sqlx::query_scalar("SELECT definition_json FROM task_step_workflow WHERE id=?")
                .bind(id)
                .fetch_one(self.pool())
                .await?,
        )
    }
    async fn reroute_step(&self, step: &TaskStep, lane: &str) -> Result<()> {
        let changed=sqlx::query("UPDATE task_step SET status='pending',lane=?,claimed_by=NULL,lease_until=NULL,attempts=attempts-1,available_at=?,updated_at=? WHERE id=? AND status='claimed' AND claimed_by=?")
            .bind(lane).bind(now_rfc3339()).bind(now_rfc3339()).bind(&step.id).bind(&step.claimed_by).execute(self.pool()).await?.rows_affected();
        if changed != 1 {
            return Err(DbError::VersionConflict);
        }
        self.domain_event_notify().notify_waiters();
        Ok(())
    }
    async fn prune_steps(
        &self,
        settled_before: &str,
        unresolved_before: &str,
        limit: i64,
    ) -> Result<u64> {
        let active: Vec<String> = self
            .task_step_activity
            .lock()
            .expect("step activity")
            .values()
            .map(|(id, _)| id.clone())
            .collect();
        let active = serde_json::to_string(&active)
            .map_err(|e| DbError::from(sqlx::Error::Decode(Box::new(e))))?;
        let limit = limit.clamp(1, 100);
        let now = now_rfc3339();
        let mut deleted = 0;
        for (statuses, before) in [
            (PRUNE_SETTLED, settled_before),
            (PRUNE_UNRESOLVED, unresolved_before),
        ] {
            deleted += sqlx::query(statuses)
                .bind(before)
                .bind(&now)
                .bind(&active)
                .bind(limit)
                .execute(self.pool())
                .await?
                .rows_affected();
        }
        sqlx::query(PRUNE_WORKFLOWS)
            .bind(settled_before)
            .bind(limit)
            .execute(self.pool())
            .await?;
        Ok(deleted)
    }
    async fn step_entry_matches(&self, step: &TaskStep) -> Result<bool> {
        Ok(sqlx::query_scalar(STEP_FENCE)
            .bind(&step.task_id)
            .bind(&step.expected_status)
            .bind(step.expected_epoch)
            .fetch_one(self.pool())
            .await?)
    }
    async fn pending_steps(&self, task_id: &str) -> Result<i64> {
        let own = crate::task_writer::current_task_step()
            .filter(|step| step.task_id == task_id)
            .map(|step| step.id);
        Ok(sqlx::query_scalar(
            "SELECT COUNT(*) FROM task_step WHERE task_id = ? AND status IN ('pending','claimed') AND (? IS NULL OR id<>?)",
        )
        .bind(task_id)
        .bind(&own).bind(&own)
        .fetch_one(self.pool())
        .await?)
    }
    async fn entry_hooks_pending(&self, task_id: &str) -> Result<bool> {
        let own = crate::task_writer::current_task_step()
            .filter(|step| step.task_id == task_id)
            .map(|step| step.id);
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM task_step s JOIN task t ON t.id = s.task_id WHERE s.task_id = ? AND (s.kind='hooks' OR (s.kind='command' AND s.lane='long')) AND s.status IN ('pending','claimed') AND (? IS NULL OR s.id<>?) AND s.expected_status = t.status AND s.expected_epoch = t.status_epoch)",
        )
        .bind(task_id)
        .bind(&own).bind(&own)
        .fetch_one(self.pool())
        .await?)
    }
    async fn claim_step(
        &self,
        owner: &str,
        task_id: Option<&str>,
        lease_until: &str,
    ) -> Result<Option<TaskStep>> {
        self.claim_step_lane(owner, task_id, None, lease_until)
            .await
    }
    async fn renew_step(&self, id: &str, owner: &str, lease_until: &str) -> Result<bool> {
        Ok(
            sqlx::query("UPDATE task_step SET lease_until=? WHERE id=? AND claimed_by=?")
                .bind(lease_until)
                .bind(id)
                .bind(owner)
                .execute(self.pool())
                .await?
                .rows_affected()
                == 1,
        )
    }
    async fn finish_step_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        s: &TaskStep,
        status: &str,
        error: Option<&str>,
    ) -> Result<()> {
        let now = now_rfc3339();
        let n = sqlx::query("UPDATE task_step SET status=?,last_error=?,completed_at=?,updated_at=? WHERE id=? AND status='claimed' AND claimed_by=? AND (lease_until > ? OR ?)")
            .bind(status).bind(error).bind(&now).bind(&now).bind(&s.id).bind(&s.claimed_by).bind(&now).bind(self.step_is_active(s))
            .execute(&mut **tx).await?.rows_affected();
        if n != 1 {
            return Err(DbError::VersionConflict);
        }
        self.observe_integration_terminal_best_effort(tx, s).await;
        // Only a hooks step is witnessed. Its settlement is authoritative;
        // the shadow never refuses it.
        if s.kind == "hooks" {
            crate::task_condition::produce_best_effort(
                tx,
                &s.task_id,
                crate::ConditionChange::Hooks,
            )
            .await;
        }
        Ok(())
    }
    async fn release_step(&self, id: &str, owner: &str) -> Result<()> {
        sqlx::query("UPDATE task_step SET lease_until=NULL,claimed_by=NULL WHERE id=? AND claimed_by=? AND status NOT IN ('pending','claimed')")
            .bind(id).bind(owner).execute(self.pool()).await?;
        self.task_step_activity
            .lock()
            .expect("step activity")
            .retain(|step_id, (_, token)| step_id != id || token != owner);
        self.domain_event_notify().notify_waiters();
        Ok(())
    }
    async fn retry_step(&self, s: &TaskStep, error: &str, available_at: &str) -> Result<()> {
        let now = now_rfc3339();
        let n = sqlx::query("UPDATE task_step SET status='pending',claimed_by=NULL,lease_until=NULL,last_error=?,available_at=?,updated_at=? WHERE id=? AND status='claimed' AND claimed_by=? AND (lease_until > ? OR ?)")
            .bind(error).bind(available_at).bind(&now).bind(&s.id).bind(&s.claimed_by).bind(&now).bind(self.step_is_active(s))
            .execute(self.pool()).await?.rows_affected();
        if n != 1 {
            return Err(DbError::VersionConflict);
        }
        Ok(())
    }
    async fn ready_step(&self, id: &str) -> Result<()> {
        sqlx::query(
            "UPDATE task_step SET available_at=?,updated_at=? WHERE id=? AND status='pending'",
        )
        .bind(now_rfc3339())
        .bind(now_rfc3339())
        .bind(id)
        .execute(self.pool())
        .await?;
        self.domain_event_notify().notify_waiters();
        Ok(())
    }
}

/// Process-local execution witness. A live future is not a dead lease after
/// laptop sleep; SQLite owner tokens still fence another process's claim.
pub struct TaskStepActivity {
    state: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, (String, String)>>>,
    id: String,
    controls: std::sync::Arc<
        std::sync::Mutex<
            std::collections::HashMap<String, std::sync::Arc<crate::task_writer::TaskStepControl>>,
        >,
    >,
}
impl Drop for TaskStepActivity {
    fn drop(&mut self) {
        self.state.lock().expect("step activity").remove(&self.id);
        self.controls
            .lock()
            .expect("step controls")
            .remove(&self.id);
    }
}
impl SqliteDb {
    /// Check lease ownership and the producing status entry in the writer
    /// transaction that records a hook effect or checkpoint.
    pub async fn fence_hook_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        step: &TaskStep,
    ) -> Result<()> {
        let owned: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_step WHERE id=? AND (status='claimed' OR (? AND status='done')) AND claimed_by=? AND (lease_until>? OR ?))")
            .bind(&step.id).bind(step.kind != "hooks").bind(&step.claimed_by).bind(now_rfc3339()).bind(self.step_is_active(step)).fetch_one(&mut **tx).await?;
        if !owned {
            return Err(DbError::VersionConflict);
        }
        Ok(())
    }

    /// Returns a recorded outcome and whether an unrecorded attempt was
    /// interrupted. Starting is durable before invoking the action.
    pub async fn start_hook(&self, step: &TaskStep, index: i64) -> Result<(Option<String>, bool)> {
        let mut tx = begin_immediate(self.pool()).await?;
        self.fence_hook_in_tx(&mut tx, step).await?;
        let previous: Option<Option<String>> = sqlx::query_scalar(
            "SELECT result_json FROM task_hook_checkpoint WHERE step_id=? AND hook_index=?",
        )
        .bind(&step.id)
        .bind(index)
        .fetch_optional(&mut *tx)
        .await?;
        sqlx::query("INSERT INTO task_hook_checkpoint(step_id,hook_index,started_at) VALUES(?,?,?) ON CONFLICT DO NOTHING")
            .bind(&step.id).bind(index).bind(now_rfc3339()).execute(&mut *tx).await?;
        tx.commit().await?;
        let interrupted = previous.as_ref().is_some_and(Option::is_none);
        Ok((previous.flatten(), interrupted))
    }

    pub async fn finish_hook(&self, step: &TaskStep, index: i64, outcome: &str) -> Result<()> {
        let mut tx = begin_immediate(self.pool()).await?;
        self.fence_hook_in_tx(&mut tx, step).await?;
        sqlx::query(
            "UPDATE task_hook_checkpoint SET result_json=? WHERE step_id=? AND hook_index=?",
        )
        .bind(outcome)
        .bind(&step.id)
        .bind(index)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn hook_effect(
        &self,
        step: &TaskStep,
        index: i64,
        key: &str,
    ) -> Result<Option<String>> {
        Ok(sqlx::query_scalar("SELECT json_extract(effects_json,?) FROM task_hook_checkpoint WHERE step_id=? AND hook_index=?")
            .bind(format!("$.{key}")).bind(&step.id).bind(index).fetch_one(self.pool()).await?)
    }
    pub async fn record_hook_effect(
        &self,
        step: &TaskStep,
        index: i64,
        key: &str,
        value: &str,
    ) -> Result<()> {
        let mut tx = begin_immediate(self.pool()).await?;
        self.fence_hook_in_tx(&mut tx, step).await?;
        sqlx::query("UPDATE task_hook_checkpoint SET effects_json=json_set(effects_json,?,?) WHERE step_id=? AND hook_index=?")
            .bind(format!("$.{key}")).bind(value).bind(&step.id).bind(index).execute(&mut *tx).await?;
        self.observe_integration_hook_best_effort(&mut tx, step, index, key, value)
            .await;
        tx.commit().await?;
        Ok(())
    }

    pub async fn start_hook_script(
        &self,
        step: &TaskStep,
        hook: i64,
        script: i64,
    ) -> Result<(Option<String>, bool)> {
        let mut tx = begin_immediate(self.pool()).await?;
        self.fence_hook_in_tx(&mut tx, step).await?;
        let previous: Option<Option<String>> = sqlx::query_scalar("SELECT result_json FROM task_hook_script WHERE step_id=? AND hook_index=? AND script_index=?")
            .bind(&step.id).bind(hook).bind(script).fetch_optional(&mut *tx).await?;
        sqlx::query("INSERT INTO task_hook_script(step_id,hook_index,script_index,started_at) VALUES(?,?,?,?) ON CONFLICT DO NOTHING")
            .bind(&step.id).bind(hook).bind(script).bind(now_rfc3339()).execute(&mut *tx).await?;
        tx.commit().await?;
        let interrupted = previous.as_ref().is_some_and(Option::is_none);
        Ok((previous.flatten(), interrupted))
    }

    pub async fn finish_hook_script(
        &self,
        step: &TaskStep,
        hook: i64,
        script: i64,
        outcome: &str,
    ) -> Result<()> {
        let mut tx = begin_immediate(self.pool()).await?;
        self.fence_hook_in_tx(&mut tx, step).await?;
        sqlx::query("UPDATE task_hook_script SET result_json=? WHERE step_id=? AND hook_index=? AND script_index=?")
            .bind(outcome).bind(&step.id).bind(hook).bind(script).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    pub fn hold_task_step(&self, step: &TaskStep) -> TaskStepActivity {
        self.task_step_activity
            .lock()
            .expect("step activity")
            .insert(
                step.id.clone(),
                (
                    step.task_id.clone(),
                    step.claimed_by.clone().expect("claimed owner"),
                ),
            );
        TaskStepActivity {
            state: self.task_step_activity.clone(),
            id: step.id.clone(),
            controls: self.task_step_controls.clone(),
        }
    }
    pub fn step_is_active(&self, step: &TaskStep) -> bool {
        self.task_step_activity
            .lock()
            .expect("step activity")
            .get(&step.id)
            .is_some_and(|(_, owner)| Some(owner) == step.claimed_by.as_ref())
    }
    pub fn task_step_is_running(&self, task_id: &str) -> bool {
        self.task_step_activity
            .lock()
            .expect("step activity")
            .values()
            .any(|(id, _)| id == task_id)
    }
    pub async fn renew_active_steps(&self, until: &str) -> Result<()> {
        let owners: Vec<(String, String)> = self
            .task_step_activity
            .lock()
            .expect("step activity")
            .iter()
            .map(|(id, (_, owner))| (id.clone(), owner.clone()))
            .collect();
        let due = (chrono::Utc::now() + chrono::Duration::seconds(30)).to_rfc3339();
        for (id, owner) in owners {
            sqlx::query(
                "UPDATE task_step SET lease_until=? WHERE id=? AND claimed_by=? AND lease_until<?",
            )
            .bind(until)
            .bind(id)
            .bind(owner)
            .bind(&due)
            .execute(self.pool())
            .await?;
        }
        Ok(())
    }
    /// Role entries about to take an agent slot: `(for agent_id, all)`,
    /// excluding `excluding_task`. Counts a Task's available fast-lane head
    /// step or a claimed fast-lane hook while dispatch is imminent. Completed
    /// status steps and recorded dispatch outcomes reserve nothing. Rows behind another
    /// step, in back-off, or waiting for or
    /// inside a long-lane merge/CI hook hold no capacity; the dispatcher
    /// re-drives a refused entry.
    pub async fn queued_admissions(
        &self,
        agent_id: &str,
        excluding_task: &str,
    ) -> Result<(i64, i64)> {
        let now = now_rfc3339();
        Ok(sqlx::query_as(
            "SELECT COALESCE(SUM(json_extract(s.payload_json,'$.admission_agent_id')=?),0),COUNT(*) FROM task_step s \
             WHERE s.task_id<>? AND json_extract(s.payload_json,'$.admission_agent_id') IS NOT NULL \
             AND ((s.status='pending' AND s.lane='fast' AND s.available_at<=? \
                   AND NOT EXISTS(SELECT 1 FROM task_step p WHERE p.task_id=s.task_id AND p.seq<s.seq AND p.status IN ('pending','claimed')) \
                   AND NOT EXISTS(SELECT 1 FROM task_step p WHERE p.task_id=s.task_id AND p.id!=s.id AND p.status='claimed' AND p.lease_until>?)) \
               OR (s.status='claimed' AND s.lane='fast' AND s.lease_until>?)) \
             AND EXISTS(SELECT 1 FROM task t WHERE t.id=s.task_id AND t.status=s.expected_status AND t.status_epoch=s.expected_epoch AND t.deleted_at IS NULL) \
             AND NOT EXISTS(SELECT 1 FROM task_hook_checkpoint h WHERE h.step_id=s.id AND (h.hook_index=json_extract(s.payload_json,'$.dispatch_index') AND h.result_json IS NOT NULL OR (json_extract(s.payload_json,'$.dispatch_index') IS NOT NULL AND json_type(h.result_json,'$.Cascade') IS NOT NULL) OR json_type(h.result_json,'$.Failed') IS NOT NULL)) \
             AND NOT EXISTS(SELECT 1 FROM execution e WHERE e.task_id=s.task_id AND e.status='running')",
        )
        .bind(agent_id)
        .bind(excluding_task)
        .bind(&now)
        .bind(&now)
        .bind(&now)
        .fetch_one(self.pool())
        .await?)
    }
    pub fn active_step_count(&self) -> usize {
        self.task_step_activity.lock().expect("step activity").len()
    }
    pub fn register_step_control(
        &self,
        step: &TaskStep,
    ) -> std::sync::Arc<crate::task_writer::TaskStepControl> {
        let control = crate::task_writer::TaskStepControl::new();
        self.task_step_controls
            .lock()
            .expect("step controls")
            .insert(step.id.clone(), control.clone());
        control
    }
    pub fn release_step_control(&self, step: &TaskStep) {
        self.task_step_controls
            .lock()
            .expect("step controls")
            .remove(&step.id);
    }
    pub async fn claim_step_lane(
        &self,
        owner: &str,
        task_id: Option<&str>,
        lane: Option<&str>,
        lease_until: &str,
    ) -> Result<Option<TaskStep>> {
        let active: Vec<String> = self
            .task_step_activity
            .lock()
            .expect("step activity")
            .values()
            .map(|(task, _)| task.clone())
            .collect();
        let active = serde_json::to_string(&active)
            .map_err(|e| DbError::from(sqlx::Error::Decode(Box::new(e))))?;
        let now = now_rfc3339();
        if let Some(lane) = lane {
            let live:i64=sqlx::query_scalar("SELECT COUNT(*) FROM task_step WHERE status='claimed' AND lane=? AND lease_until>?")
                .bind(lane).bind(&now).fetch_one(self.pool()).await?;
            if live >= if lane == "long" { 4 } else { 8 } {
                return Ok(None);
            }
        }
        let candidate = "SELECT s.* FROM task_step s WHERE s.status IN ('pending','claimed') AND (? IS NULL OR s.lane=?) AND s.task_id NOT IN (SELECT value FROM json_each(?)) AND (? IS NULL OR s.task_id = ?) AND ((s.status = 'pending' AND s.available_at <= ?) OR (s.status = 'claimed' AND s.lease_until <= ?)) AND NOT EXISTS (SELECT 1 FROM task_step p WHERE p.task_id = s.task_id AND ((p.integration_started_at IS NOT NULL AND p.seq<s.seq) OR (s.integration_started_at IS NULL AND p.priority>s.priority) OR (p.priority=s.priority AND p.seq<s.seq)) AND p.status IN ('pending','claimed')) AND NOT EXISTS (SELECT 1 FROM task_step p WHERE p.task_id = s.task_id AND p.id != s.id AND p.lease_until > ?) ORDER BY s.priority DESC,s.created_at,s.seq LIMIT 1";
        if sqlx::query(candidate)
            .bind(lane)
            .bind(lane)
            .bind(&active)
            .bind(task_id)
            .bind(task_id)
            .bind(&now)
            .bind(&now)
            .bind(&now)
            .fetch_optional(self.pool())
            .await?
            .is_none()
        {
            return Ok(None);
        }
        let mut tx = begin_immediate(self.pool()).await?;
        if let Some(lane) = lane {
            let live: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_step WHERE status='claimed' AND lane=? AND lease_until>?")
                .bind(lane).bind(&now).fetch_one(&mut *tx).await?;
            if live >= if lane == "long" { 4 } else { 8 } {
                tx.commit().await?;
                return Ok(None);
            }
        }
        let row = sqlx::query(candidate)
            .bind(lane)
            .bind(lane)
            .bind(&active)
            .bind(task_id)
            .bind(task_id)
            .bind(&now)
            .bind(&now)
            .bind(&now)
            .fetch_optional(&mut *tx)
            .await?;
        let Some(row) = row else {
            tx.commit().await?;
            return Ok(None);
        };
        let id: String = row.get("id");
        sqlx::query("UPDATE task_step SET status='claimed',claimed_by=?,lease_until=?,attempts=attempts+1,updated_at=? WHERE id=?")
            .bind(owner).bind(lease_until).bind(&now).bind(&id).execute(&mut *tx).await?;
        let row = sqlx::query("SELECT * FROM task_step WHERE id=?")
            .bind(&id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(Some(row_step(row)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TaskRepo;
    async fn fixture() -> SqliteDb {
        let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
        crate::run_migrations(&pool).await.unwrap();
        let db = SqliteDb::new(pool);
        let now = now_rfc3339();
        sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES ('p','p',?,?)")
            .bind(&now)
            .bind(&now)
            .execute(db.pool())
            .await
            .unwrap();
        for id in ["a", "b"] {
            sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES (?,'p',?,'todo',?,?)")
                .bind(id).bind(id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        }
        db
    }
    fn input(task: &str, key: &str) -> EnqueueTaskStep {
        EnqueueTaskStep {
            kind: "cascade".into(),
            id: crate::new_uuid_v4(),
            task_id: task.into(),
            payload_json: "{}".into(),
            causation_step_id: None,
            causation_key: key.into(),
            chain_id: "chain".into(),
            chain_position: 1,
            expected_status: "todo".into(),
            expected_version: 1,
            expected_epoch: None,
            lane: "fast".into(),
            available_at: now_rfc3339(),
        }
    }
    fn later() -> String {
        (chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339()
    }
    #[tokio::test]
    async fn fast_commands_do_not_hide_owner_actions_but_long_commands_do() {
        let db = fixture().await;
        let mut fast = input("a", "fast-command");
        fast.kind = "command".into();
        db.enqueue_step(&fast).await.unwrap();
        assert!(!db.entry_hooks_pending("a").await.unwrap());
        let mut long = input("a", "long-command");
        long.kind = "command".into();
        long.lane = "long".into();
        db.enqueue_step(&long).await.unwrap();
        assert!(db.entry_hooks_pending("a").await.unwrap());
    }
    #[tokio::test]
    async fn enqueue_rolls_back_with_producing_cas_and_deduplicates() {
        let db = fixture().await;
        let i = input("a", "one");
        let mut tx = begin_immediate(db.pool()).await.unwrap();
        sqlx::query(
            "UPDATE task SET status='planning',version=version+1 WHERE id='a' AND version=1",
        )
        .execute(&mut *tx)
        .await
        .unwrap();
        db.enqueue_step_in_tx(&mut tx, &i).await.unwrap();
        tx.rollback().await.unwrap();
        assert_eq!(db.pending_steps("a").await.unwrap(), 0);
        assert_eq!(db.enqueue_step(&i).await.unwrap(), i.id);
        let mut duplicate = i.clone();
        duplicate.id = crate::new_uuid_v4();
        assert_eq!(db.enqueue_step(&duplicate).await.unwrap(), i.id);
        assert_eq!(db.task_steps("a").await.unwrap().len(), 1);
    }
    #[tokio::test]
    async fn fifo_claims_one_per_task_but_other_tasks_progress() {
        let db = fixture().await;
        db.enqueue_step(&input("a", "one")).await.unwrap();
        db.enqueue_step(&input("a", "two")).await.unwrap();
        db.enqueue_step(&input("b", "one")).await.unwrap();
        let a = db
            .claim_step("owner-a", Some("a"), &later())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(a.seq, 1);
        assert!(db
            .claim_step("other", Some("a"), &later())
            .await
            .unwrap()
            .is_none());
        assert!(db
            .claim_step("owner-b", Some("b"), &later())
            .await
            .unwrap()
            .is_some());
        let mut tx = begin_immediate(db.pool()).await.unwrap();
        db.finish_step_in_tx(&mut tx, &a, "done", None)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        // Done at CAS still protects the predecessor's inline hook phase.
        assert!(db
            .claim_step("other", Some("a"), &later())
            .await
            .unwrap()
            .is_none());
        db.release_step(&a.id, "owner-a").await.unwrap();
        assert_eq!(
            db.claim_step("other", Some("a"), &later())
                .await
                .unwrap()
                .unwrap()
                .seq,
            2
        );
    }
    #[tokio::test]
    async fn expired_claim_is_reclaimed_and_old_owner_cannot_commit() {
        let db = fixture().await;
        db.enqueue_step(&input("a", "one")).await.unwrap();
        let old = db
            .claim_step("old", Some("a"), &later())
            .await
            .unwrap()
            .unwrap();
        sqlx::query("UPDATE task_step SET lease_until='2000-01-01T00:00:00+00:00' WHERE id=?")
            .bind(&old.id)
            .execute(db.pool())
            .await
            .unwrap();
        let replacement = SqliteDb::new(db.pool().clone());
        let new = replacement
            .claim_step("new", Some("a"), &later())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(new.id, old.id);
        assert_eq!(new.attempts, 2);
        let mut tx = begin_immediate(db.pool()).await.unwrap();
        assert!(matches!(
            db.finish_step_in_tx(&mut tx, &old, "done", None).await,
            Err(DbError::VersionConflict)
        ));
        tx.rollback().await.unwrap();
        assert!(!db.renew_step(&old.id, "old", &later()).await.unwrap());
    }
    #[tokio::test]
    async fn done_and_status_cas_rollback_together() {
        let db = fixture().await;
        db.enqueue_step(&input("a", "one")).await.unwrap();
        let step = db
            .claim_step("owner", Some("a"), &later())
            .await
            .unwrap()
            .unwrap();
        let mut tx = begin_immediate(db.pool()).await.unwrap();
        sqlx::query(
            "UPDATE task SET status='planning',version=version+1 WHERE id='a' AND version=1",
        )
        .execute(&mut *tx)
        .await
        .unwrap();
        db.finish_step_in_tx(&mut tx, &step, "done", None)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        assert_eq!(db.task_steps("a").await.unwrap()[0].status, "claimed");
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM task WHERE id='a'")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            "todo"
        );
    }
    #[tokio::test]
    async fn retry_backoff_preserves_fifo_head_and_other_tasks_progress() {
        let db = fixture().await;
        db.enqueue_step(&input("a", "one")).await.unwrap();
        db.enqueue_step(&input("a", "two")).await.unwrap();
        db.enqueue_step(&input("b", "one")).await.unwrap();
        let first = db
            .claim_step("owner", Some("a"), &later())
            .await
            .unwrap()
            .unwrap();
        db.retry_step(&first, "temporary refusal", &later())
            .await
            .unwrap();
        assert!(db
            .claim_step("other", Some("a"), &later())
            .await
            .unwrap()
            .is_none());
        assert!(db
            .claim_step("other", Some("b"), &later())
            .await
            .unwrap()
            .is_some());
        sqlx::query("UPDATE task_step SET available_at='2000-01-01T00:00:00+00:00' WHERE id=?")
            .bind(&first.id)
            .execute(db.pool())
            .await
            .unwrap();
        let retried = db
            .claim_step("new", Some("a"), &later())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retried.id, first.id);
        assert_eq!(retried.seq, 1);
        assert_eq!(retried.attempts, 2);
        assert_eq!(retried.last_error.as_deref(), Some("temporary refusal"));
    }
    #[tokio::test]
    async fn epoch_fence_tolerates_edits_and_markers_but_not_a_return_to_same_status() {
        let db = fixture().await;
        let id = db.enqueue_step(&input("a", "edit")).await.unwrap();
        let step = db
            .task_steps("a")
            .await
            .unwrap()
            .into_iter()
            .find(|s| s.id == id)
            .unwrap();
        assert_eq!(step.expected_epoch, 0);
        sqlx::query("UPDATE task SET title='edited',version=version+1 WHERE id='a'")
            .execute(db.pool())
            .await
            .unwrap();
        // A same-state recovery marker or reorder audit row is not an entry.
        sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,created_at) VALUES('marker','a','todo','todo','user','retry window reset',?)").bind(now_rfc3339()).execute(db.pool()).await.unwrap();
        sqlx::query("UPDATE task SET status='todo',version=version+1 WHERE id='a'")
            .execute(db.pool())
            .await
            .unwrap();
        assert!(db.step_entry_matches(&step).await.unwrap());
        // Leaving and returning, by any writer, is a new entry.
        for status in ["planning", "todo"] {
            sqlx::query("UPDATE task SET status=? WHERE id='a'")
                .bind(status)
                .execute(db.pool())
                .await
                .unwrap();
        }
        let epoch: i64 = sqlx::query_scalar("SELECT status_epoch FROM task WHERE id='a'")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(epoch, 2);
        assert!(!db.step_entry_matches(&step).await.unwrap());
        // An in-transaction enqueue reads the epoch after the producing CAS.
        let mut tx = begin_immediate(db.pool()).await.unwrap();
        sqlx::query("UPDATE task SET status='planning' WHERE id='a'")
            .execute(&mut *tx)
            .await
            .unwrap();
        let mut produced = input("a", "produced");
        produced.expected_status = "planning".into();
        db.enqueue_step_in_tx(&mut tx, &produced).await.unwrap();
        tx.commit().await.unwrap();
        let produced = db
            .task_steps("a")
            .await
            .unwrap()
            .into_iter()
            .find(|s| s.causation_key == "produced")
            .unwrap();
        assert_eq!(produced.expected_epoch, 3);
        assert!(db.step_entry_matches(&produced).await.unwrap());
    }
    #[tokio::test]
    async fn live_hook_witness_prevents_wall_expired_reclaim_and_fences_owner() {
        let db = fixture().await;
        db.enqueue_step(&input("a", "wake")).await.unwrap();
        let step = db
            .claim_step("owner", Some("a"), "2000-01-01T00:00:00+00:00")
            .await
            .unwrap()
            .unwrap();
        let activity = db.hold_task_step(&step);
        assert!(db
            .claim_step("other", Some("a"), &later())
            .await
            .unwrap()
            .is_none());
        let mut tx = begin_immediate(db.pool()).await.unwrap();
        db.finish_step_in_tx(&mut tx, &step, "done", None)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        db.renew_step(&step.id, "owner", &later()).await.unwrap();
        drop(activity);
        db.release_step(&step.id, "owner").await.unwrap();
        assert_eq!(db.task_steps("a").await.unwrap()[0].status, "done");
    }
    #[tokio::test]
    async fn retention_prunes_only_old_completed_inactive_chains_in_batches() {
        let db = fixture().await;
        for n in 0..105 {
            let mut value = input("a", &format!("done-{n}"));
            value.chain_id = format!("chain-{n}");
            db.enqueue_step(&value).await.unwrap();
        }
        sqlx::query("UPDATE task_step SET status='done',completed_at='2000-01-01T00:00:00Z'")
            .execute(db.pool())
            .await
            .unwrap();
        db.enqueue_step(&input("b", "pending")).await.unwrap();
        assert_eq!(
            db.prune_steps("2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z", 1000)
                .await
                .unwrap(),
            100
        );
        assert_eq!(db.task_steps("a").await.unwrap().len(), 5);
        assert_eq!(db.pending_steps("b").await.unwrap(), 1);
    }
    #[tokio::test]
    async fn retention_keeps_unresolved_rows_longer_and_uses_indexes() {
        let db = fixture().await;
        let workflow = db.store_step_workflow("{\"states\":[]}").await.unwrap();
        for (key, status, completed) in [
            ("done-old", "done", "2000-01-01T00:00:00Z"),
            ("failed-recent", "failed", "2020-01-20T00:00:00Z"),
            ("parked-old", "parked", "2000-01-01T00:00:00Z"),
        ] {
            let mut value = input("a", key);
            value.chain_id = key.into();
            if status == "failed" {
                value.payload_json =
                    serde_json::json!({"workflow_ref":{"kind":"snapshot","id":workflow}})
                        .to_string();
            }
            db.enqueue_step(&value).await.unwrap();
            sqlx::query("UPDATE task_step SET status=?,completed_at=? WHERE causation_key=?")
                .bind(status)
                .bind(completed)
                .bind(key)
                .execute(db.pool())
                .await
                .unwrap();
        }
        sqlx::query("UPDATE task_step_workflow SET last_used_at='2000-01-01T00:00:00Z'")
            .execute(db.pool())
            .await
            .unwrap();
        // Seven-day cutoff for settled rows, thirty-day cutoff for unresolved.
        assert_eq!(
            db.prune_steps("2020-01-25T00:00:00Z", "2020-01-01T00:00:00Z", 100)
                .await
                .unwrap(),
            2
        );
        let left: Vec<String> = db
            .task_steps("a")
            .await
            .unwrap()
            .into_iter()
            .map(|s| s.causation_key)
            .collect();
        assert_eq!(left, vec!["failed-recent".to_owned()]);
        // The failed row still references its definition.
        assert!(db.step_workflow(&workflow).await.is_ok());
        db.prune_steps("2020-01-25T00:00:00Z", "2020-02-01T00:00:00Z", 100)
            .await
            .unwrap();
        assert!(db.task_steps("a").await.unwrap().is_empty());
        assert!(db.step_workflow(&workflow).await.is_err());
        for (sql, index) in [
            (PRUNE_SETTLED, "task_step_settled"),
            (PRUNE_UNRESOLVED, "task_step_settled"),
            (PRUNE_WORKFLOWS, "task_step_workflow_ref"),
        ] {
            let explain = format!("EXPLAIN QUERY PLAN {sql}");
            let mut query = sqlx::query(&explain).bind("2020-01-01T00:00:00Z");
            if sql == PRUNE_WORKFLOWS {
                query = query.bind(100);
            } else {
                query = query.bind(now_rfc3339()).bind("[]").bind(100);
            }
            let plan: Vec<String> = query
                .fetch_all(db.pool())
                .await
                .unwrap()
                .into_iter()
                .map(|row| row.get::<String, _>("detail"))
                .collect();
            assert!(
                plan.iter().any(|detail| detail.contains(index)),
                "{index} not used: {plan:?}"
            );
        }
    }
    #[tokio::test]
    async fn hook_retry_after_sleep_lands_for_a_live_local_owner() {
        let db = fixture().await;
        db.enqueue_step(&input("a", "sleep")).await.unwrap();
        let step = db
            .claim_step("owner", Some("a"), "2000-01-01T00:00:00+00:00")
            .await
            .unwrap()
            .unwrap();
        let activity = db.hold_task_step(&step);
        db.retry_step(&step, "transient after wake", &later())
            .await
            .unwrap();
        let row = &db.task_steps("a").await.unwrap()[0];
        assert_eq!(row.status, "pending");
        assert_eq!(row.last_error.as_deref(), Some("transient after wake"));
        drop(activity);
        // Without the local witness an expired owner is fenced out.
        let stale = db
            .claim_step("stale", Some("a"), "2000-01-01T00:00:00+00:00")
            .await
            .unwrap();
        assert!(stale.is_none(), "back-off keeps the step pending");
        sqlx::query("UPDATE task_step SET available_at='2000-01-01T00:00:00Z'")
            .execute(db.pool())
            .await
            .unwrap();
        let stale = db
            .claim_step("stale", Some("a"), "2000-01-01T00:00:00+00:00")
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            db.retry_step(&stale, "lost", &later()).await,
            Err(DbError::VersionConflict)
        ));
    }
    #[tokio::test]
    async fn queued_admissions_count_only_imminent_role_entries() {
        let db = fixture().await;
        let now = now_rfc3339();
        for id in ["c", "d", "e", "f"] {
            sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES (?,'p',?,'todo',?,?)")
                .bind(id).bind(id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        }
        let role_entry = |task: &str, key: &str, lane: &str| {
            let mut value = input(task, key);
            value.payload_json = serde_json::json!({"admission_agent_id":"agent"}).to_string();
            value.lane = lane.into();
            value
        };
        // a: available head entry -> counts.
        db.enqueue_step(&role_entry("a", "head", "fast"))
            .await
            .unwrap();
        assert_eq!(db.queued_admissions("agent", "z").await.unwrap(), (1, 1));
        // b: a long-lane step inside its CI hook, with an entry queued behind it.
        db.enqueue_step(&role_entry("b", "ci", "long"))
            .await
            .unwrap();
        db.enqueue_step(&role_entry("b", "behind", "fast"))
            .await
            .unwrap();
        db.claim_step("ci-owner", Some("b"), &later())
            .await
            .unwrap()
            .unwrap();
        // g: an available long-lane head waiting for a long slot (CI first).
        sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES ('g','p','g','todo',?,?)")
            .bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        db.enqueue_step(&role_entry("g", "long-head", "long"))
            .await
            .unwrap();
        // c: in retry back-off; d: under its producer's reservation.
        let mut backoff = role_entry("c", "backoff", "fast");
        backoff.available_at = later();
        db.enqueue_step(&backoff).await.unwrap();
        let mut reserved = role_entry("d", "reserved", "fast");
        reserved.available_at = later();
        db.enqueue_step(&reserved).await.unwrap();
        assert_eq!(db.queued_admissions("agent", "z").await.unwrap(), (1, 1));
        // e: claimed fast entry; f: done, still leased for its inline dispatch.
        db.enqueue_step(&role_entry("e", "dispatching", "fast"))
            .await
            .unwrap();
        db.claim_step("e-owner", Some("e"), &later())
            .await
            .unwrap()
            .unwrap();
        db.enqueue_step(&role_entry("f", "committed", "fast"))
            .await
            .unwrap();
        let committed = db
            .claim_step("f-owner", Some("f"), &later())
            .await
            .unwrap()
            .unwrap();
        let mut tx = begin_immediate(db.pool()).await.unwrap();
        db.finish_step_in_tx(&mut tx, &committed, "done", None)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(db.queued_admissions("agent", "z").await.unwrap(), (2, 2));
        db.release_step(&committed.id, "f-owner").await.unwrap();
        let mut hooks = role_entry("f", "hook-dispatch", "fast");
        hooks.kind = "hooks".into();
        hooks.payload_json =
            serde_json::json!({"admission_agent_id":"agent", "dispatch_index":0}).to_string();
        db.enqueue_step(&hooks).await.unwrap();
        let hook_step = db
            .claim_step("hook-owner", Some("f"), &later())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(db.queued_admissions("agent", "z").await.unwrap(), (3, 3));
        assert_eq!(db.queued_admissions("other", "a").await.unwrap(), (0, 2));
        db.start_hook(&hook_step, 0).await.unwrap();
        db.finish_hook(&hook_step, 0, "\"Ok\"").await.unwrap();
        assert_eq!(db.queued_admissions("agent", "z").await.unwrap(), (2, 2));
    }
    /// A cascade settles in its commit but keeps its lease until its owner
    /// releases it. The entry it queued is already the Task's imminent
    /// admission: hiding it for that window let a second waiter past a full
    /// machine in the same dispatcher pass.
    #[tokio::test]
    async fn queued_admissions_count_an_entry_behind_a_settled_leased_step() {
        let db = fixture().await;
        let mut cascade = input("a", "cascade");
        cascade.payload_json = serde_json::json!({"admission_agent_id":"agent"}).to_string();
        db.enqueue_step(&cascade).await.unwrap();
        let settled = db
            .claim_step("owner", Some("a"), &later())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(db.queued_admissions("agent", "z").await.unwrap(), (1, 1));
        let mut tx = begin_immediate(db.pool()).await.unwrap();
        db.finish_step_in_tx(&mut tx, &settled, "done", None)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let mut entry = input("a", "entry");
        entry.kind = "hooks".into();
        entry.payload_json =
            serde_json::json!({"admission_agent_id":"agent", "dispatch_index":0}).to_string();
        db.enqueue_step(&entry).await.unwrap();
        // Settled, lease not yet released.
        assert_eq!(db.queued_admissions("agent", "z").await.unwrap(), (1, 1));
        db.release_step(&settled.id, "owner").await.unwrap();
        assert_eq!(db.queued_admissions("agent", "z").await.unwrap(), (1, 1));
    }
    #[tokio::test]
    async fn hook_and_script_checkpoints_reject_stale_owners_and_serialize_writers() {
        let db = fixture().await;
        let mut input = input("a", "hook");
        input.kind = "hooks".into();
        db.enqueue_step(&input).await.unwrap();
        let step = db
            .claim_step("owner", Some("a"), &later())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(db.start_hook(&step, 0).await.unwrap(), (None, false));
        assert_eq!(db.start_hook(&step, 0).await.unwrap(), (None, true));
        assert_eq!(
            db.start_hook_script(&step, 0, 0).await.unwrap(),
            (None, false)
        );
        assert_eq!(
            db.start_hook_script(&step, 0, 0).await.unwrap(),
            (None, true)
        );
        db.finish_hook_script(&step, 0, 0, "script-result")
            .await
            .unwrap();
        assert_eq!(
            db.start_hook_script(&step, 0, 0).await.unwrap(),
            (Some("script-result".into()), false)
        );
        db.record_hook_effect(&step, 0, "merge", "{\"Done\":true}")
            .await
            .unwrap();
        assert_eq!(
            db.hook_effect(&step, 0, "merge").await.unwrap().as_deref(),
            Some("{\"Done\":true}")
        );
        sqlx::query("UPDATE task_step SET lease_until='2000-01-01T00:00:00Z' WHERE id=?")
            .bind(&step.id)
            .execute(db.pool())
            .await
            .unwrap();
        let replacement = db
            .claim_step("replacement", Some("a"), &later())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            db.finish_hook(&step, 0, "old").await,
            Err(DbError::VersionConflict)
        ));
        db.enqueue_task_mutation(
            "a",
            crate::TaskMutation::Sql {
                task_id: "a".into(),
                query: "UPDATE task SET status='other',version=version+1 WHERE id=?".into(),
                arguments: vec![serde_json::json!("a")],
            },
        )
        .await
        .unwrap();
        assert_eq!(
            TaskRepo::get_by_id(&db, "a", false)
                .await
                .unwrap()
                .unwrap()
                .status,
            "todo"
        );
        db.finish_hook_script(&replacement, 0, 0, "current")
            .await
            .unwrap();
        let mut tx = begin_immediate(db.pool()).await.unwrap();
        db.finish_step_in_tx(&mut tx, &replacement, "done", None)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        db.release_step(&replacement.id, "replacement")
            .await
            .unwrap();
        let next = db
            .claim_step("writer", Some("a"), &later())
            .await
            .unwrap()
            .unwrap();
        crate::task_writer::in_task_step(next.clone(), db.execute_task_mutation(&next))
            .await
            .unwrap();
        assert_eq!(
            TaskRepo::get_by_id(&db, "a", false)
                .await
                .unwrap()
                .unwrap()
                .status,
            "other"
        );
    }
    #[tokio::test]
    async fn durable_hook_upgrade_unblocks_legacy_running_barrier_without_moving_epoch() {
        let db = fixture().await;
        sqlx::query("UPDATE task SET entry_barrier_json=?,title='preserved',version=7 WHERE id='a'")
            .bind(serde_json::json!({"status":"running","state":"todo","started_at":"2026-10-01T00:00:00Z","infrastructure_attempts":2}).to_string()).execute(db.pool()).await.unwrap();
        let sql = include_str!("../migrations/V202610040225__durable_hook_steps.sql")
            .split("-- Legacy running barriers")
            .nth(1)
            .unwrap();
        let sql = &sql[sql.find("UPDATE task SET").unwrap()..];
        sqlx::raw_sql(sql).execute(db.pool()).await.unwrap();
        let (title, version, epoch, raw): (String, i64, i64, String) = sqlx::query_as(
            "SELECT title,version,status_epoch,entry_barrier_json FROM task WHERE id='a'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(title, "preserved");
        assert_eq!(version, 8);
        assert_eq!(epoch, 0);
        let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(value["status"], "blocked");
        assert_eq!(value["infrastructure_attempts"], 2);
    }

    // A real upgrade from the pre-durable-hooks schema with in-flight step rows.
    #[tokio::test]
    async fn upgrade_preserves_in_flight_steps_and_converts_running_barrier() {
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
        let old = tempfile::TempDir::new().unwrap();
        for entry in std::fs::read_dir(&src).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().to_string_lossy().to_string();
            if name.ends_with(".sql") && name.as_str() < "V202610040225" {
                std::fs::copy(entry.path(), old.path().join(&name)).unwrap();
            }
        }
        let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
        crate::run_migrations_from(&pool, old.path()).await.unwrap();
        let now = now_rfc3339();
        sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES ('p','p',?,?)")
            .bind(&now)
            .bind(&now)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at,entry_barrier_json) VALUES ('a','p','a','review',?,?,?)")
            .bind(&now).bind(&now).bind(serde_json::json!({"status":"running","state":"review"}).to_string()).execute(&pool).await.unwrap();
        for (id, seq, status, cause, lease) in [
            ("s1", 1, "done", None, Some("2099-01-01T00:00:00Z")),
            ("s2", 2, "claimed", Some("s1"), Some("2099-01-01T00:00:00Z")),
            ("s3", 3, "pending", Some("s2"), None),
        ] {
            sqlx::query("INSERT INTO task_step(id,task_id,seq,kind,payload_json,causation_step_id,causation_key,chain_id,chain_position,expected_status,expected_version,status,claimed_by,lease_until,available_at,created_at,updated_at,expected_epoch,lane) VALUES (?,'a',?,'cascade',?,?,?,'c',?,'review',1,?,?,?,?,?,?,4,'long')")
                .bind(id).bind(seq).bind(r#"{"workflow_ref":{"id":"wf"}}"#).bind(cause).bind(format!("k{seq}")).bind(seq).bind(status)
                .bind(lease.map(|_| "owner")).bind(lease).bind(&now).bind(&now).bind(&now).execute(&pool).await.unwrap();
        }
        crate::run_migrations_from(&pool, &src).await.unwrap();
        type UpgradedStep = (
            String,
            String,
            Option<String>,
            Option<String>,
            i64,
            String,
            Option<String>,
        );
        let rows: Vec<UpgradedStep> = sqlx::query_as(
            "SELECT id,status,causation_step_id,lease_until,expected_epoch,lane,workflow_ref_id FROM task_step ORDER BY seq").fetch_all(&pool).await.unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1].2.as_deref(), Some("s1"));
        assert_eq!(rows[1].3.as_deref(), Some("2099-01-01T00:00:00Z"));
        assert_eq!(rows[2].4, 4);
        assert_eq!(rows[2].6.as_deref(), Some("wf"));
        let (barrier, ann): (String, Option<String>) =
            sqlx::query_as("SELECT entry_barrier_json,error_annotation FROM task WHERE id='a'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(barrier.contains("blocked"));
        assert!(ann.is_some_and(|a| a.contains("before_work_hook_failed")));
        let fk: Vec<(String,)> =
            sqlx::query_as("SELECT sql FROM sqlite_master WHERE name='task_step'")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert!(
            !fk[0].0.contains("task_step_new"),
            "self-reference not renamed: {}",
            fk[0].0
        );
    }
    #[tokio::test]
    async fn retiring_a_queued_admission_kicks_capacity_waiters_without_an_execution() {
        let db = fixture().await;
        let mut first = input("a", "queued-role");
        first.kind = "command".into();
        first.payload_json =
            serde_json::json!({"operation":"reconcile_role","admission_agent_id":"agent-a"})
                .to_string();
        db.enqueue_step(&first).await.unwrap();
        db.schedule_wait("b", ("p", false), Some("agent-a"), None, None)
            .await
            .unwrap();
        sqlx::query("DELETE FROM task_schedule_dirty")
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE task_step SET status='done' WHERE id=?")
            .bind(&first.id)
            .execute(db.pool())
            .await
            .unwrap();
        let mut ids = db.dirty_schedule_tasks(100).await.unwrap();
        ids.sort();
        assert_eq!(ids, ["a", "b"]);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM execution")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            0
        );
    }
}
