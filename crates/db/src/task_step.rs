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
    /// Set while `suspended`: the latest time the step looks again.
    pub suspended_until: Option<String>,
    /// The check consumer a hooks step waits for, or last waited for.
    pub awaited_consumer_id: Option<String>,
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

/// Workflow authority carried by a hooks or cascade payload. A definition is
/// preparation-only: enqueue freezes it to a snapshot in the step transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum WorkflowReference {
    Project,
    Snapshot(String),
    Definition(String),
}

async fn store_step_workflow_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    definition: &str,
) -> Result<String> {
    sqlx::query("INSERT INTO task_step_workflow(id,definition_json,last_used_at) VALUES (?,?,?) ON CONFLICT(definition_json) DO UPDATE SET last_used_at=excluded.last_used_at")
        .bind(crate::new_uuid_v4()).bind(definition).bind(now_rfc3339())
        .execute(&mut **tx).await?;
    Ok(
        sqlx::query_scalar("SELECT id FROM task_step_workflow WHERE definition_json=?")
            .bind(definition)
            .fetch_one(&mut **tx)
            .await?,
    )
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
    /// Steps that are queued or running. A `suspended` step is neither: it
    /// waits for a check result and nothing can advance it meanwhile.
    async fn pending_steps(&self, task_id: &str) -> Result<i64>;
    /// The current entry's post-commit hooks row is still pending, claimed
    /// or suspended on a check: its entry checks (CI, before-work scripts, dispatch) have not
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
        suspended_until: row.get("suspended_until"),
        awaited_consumer_id: row.get("awaited_consumer_id"),
    }
}

#[async_trait]
impl TaskStepRepo for SqliteDb {
    async fn enqueue_step_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        i: &EnqueueTaskStep,
    ) -> Result<String> {
        let mut payload_json = i.payload_json.clone();
        let definition_payload = serde_json::from_str::<serde_json::Value>(&payload_json)
            .ok()
            .filter(|payload| {
                payload
                    .pointer("/workflow_ref/kind")
                    .and_then(serde_json::Value::as_str)
                    == Some("definition")
            });
        if let Some(mut payload) = definition_payload {
            // A replay must not freeze a payload the existing step will
            // never consume. Other step kinds keep their replay handling.
            if let Some(id) = sqlx::query_scalar::<_, String>(
                "SELECT id FROM task_step WHERE task_id=? AND causation_key=?",
            )
            .bind(&i.task_id)
            .bind(&i.causation_key)
            .fetch_optional(&mut **tx)
            .await?
            {
                return Ok(id);
            }
            let definition = payload
                .pointer("/workflow_ref/id")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    DbError::Check("step workflow definition must be a string".into())
                })?;
            let id = store_step_workflow_in_tx(tx, definition).await?;
            payload["workflow_ref"] = serde_json::json!({"kind":"snapshot","id":id});
            payload_json = payload.to_string();
        }
        let now = now_rfc3339();
        sqlx::query("INSERT INTO task_step (id,task_id,seq,kind,payload_json,causation_step_id,causation_key,chain_id,chain_position,expected_status,expected_version,expected_epoch,lane,status,available_at,created_at,updated_at) SELECT ?,?,COALESCE(MAX(seq),0)+1,?,?,?,?,?,?,?,?,COALESCE(?,(SELECT status_epoch FROM task WHERE id=?),0),?,'pending',?,?,? FROM task_step WHERE task_id = ? ON CONFLICT(task_id,causation_key) DO NOTHING")
            .bind(&i.id).bind(&i.task_id).bind(&i.kind).bind(&payload_json).bind(&i.causation_step_id).bind(&i.causation_key)
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
            superseded = sqlx::query("UPDATE task_step SET status='superseded',last_error='preempted by owner command',completed_at=?,updated_at=? WHERE task_id=? AND seq<(SELECT seq FROM task_step WHERE id=?) AND priority=0 AND integration_started_at IS NULL AND entry_fenced=1 AND status IN ('pending','suspended') AND kind IN ('hooks','cascade','command','mutation','integration')")
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
        let mut tx = begin_immediate(self.pool()).await?;
        let id = store_step_workflow_in_tx(&mut tx, definition).await?;
        tx.commit().await?;
        Ok(id)
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
            "SELECT EXISTS(SELECT 1 FROM task_step s JOIN task t ON t.id = s.task_id WHERE s.task_id = ? AND (s.kind='hooks' OR (s.kind='command' AND s.lane='long')) AND s.status IN ('pending','claimed','suspended') AND (? IS NULL OR s.id<>?) AND s.expected_status = t.status AND s.expected_epoch = t.status_epoch)",
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
use crate::check_run::CANCEL_UNWATCHED_QUEUED_RUNS;

impl SqliteDb {
    pub async fn task_command_pending(&self, task_id: &str, operation: &str) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_step WHERE task_id=? AND kind='command' AND status IN ('pending','claimed') AND json_extract(payload_json,'$.operation')=?)")
            .bind(task_id).bind(operation).fetch_one(self.pool()).await?)
    }
    /// Startup only, before any Task worker runs. No claim from the previous
    /// process can still own local work. Keep checkpoints, attempts and replies;
    /// unfinished steps become immediately claimable and settled replies unlock.
    /// A claim this process holds (an inline Task writer that ran before the
    /// workers started) is live work and keeps its owner. A second process on
    /// the same database is excluded by the data root's `runtime.lock`.
    pub async fn release_orphaned_task_step_claims(&self) -> Result<u64> {
        let now = now_rfc3339();
        let live: Vec<String> = self
            .task_step_activity
            .lock()
            .expect("step activity")
            .values()
            .map(|(_, owner)| owner.clone())
            .collect();
        let live = serde_json::to_string(&live).map_err(|e| DbError::Check(e.to_string()))?;
        let changed = sqlx::query("UPDATE task_step SET status=CASE WHEN status='claimed' THEN 'pending' ELSE status END,claimed_by=NULL,lease_until=NULL,available_at=CASE WHEN status='claimed' THEN ? ELSE available_at END,updated_at=? WHERE CASE WHEN claimed_by IS NULL THEN lease_until IS NOT NULL ELSE claimed_by NOT IN (SELECT value FROM json_each(?)) END")
            .bind(&now).bind(&now).bind(&live).execute(self.pool()).await?.rows_affected();
        self.domain_event_notify().notify_waiters();
        Ok(changed)
    }

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

    /// Suspend a claimed hooks step that waits for a check result. The step
    /// keeps its open checkpoint, gives up its lease and is not charged the
    /// attempt. It is `suspended`, not `pending`: nothing claims it and it is
    /// not an unfinished step, but it still owns its status entry. The
    /// awaited consumer's delivery wakes it ([`Self::wake_suspended_hooks`]);
    /// `until` is the latest time it looks again on its own.
    pub async fn suspend_hook_step(
        &self,
        s: &TaskStep,
        until: &str,
        consumer_id: &str,
    ) -> Result<()> {
        let now = now_rfc3339();
        let n = sqlx::query("UPDATE task_step SET status='suspended',claimed_by=NULL,lease_until=NULL,attempts=MAX(attempts-1,0),last_error=NULL,suspended_until=?,awaited_consumer_id=?,updated_at=? WHERE id=? AND kind='hooks' AND status='claimed' AND claimed_by=? AND (lease_until > ? OR ?)")
            .bind(until).bind(consumer_id).bind(&now).bind(&s.id).bind(&s.claimed_by).bind(&now).bind(self.step_is_active(s))
            .execute(self.pool()).await?.rows_affected();
        if n != 1 {
            return Err(DbError::VersionConflict);
        }
        self.domain_event_notify().notify_waiters();
        Ok(())
    }

    /// The awaited consumer was answered: its suspended hooks step is
    /// `pending` again and claimable now. Returns how many steps were woken
    /// (0 when none waits any more).
    pub async fn wake_suspended_hooks(&self, task_id: &str, consumer_id: &str) -> Result<u64> {
        let now = now_rfc3339();
        let n = sqlx::query("UPDATE task_step SET status='pending',suspended_until=NULL,available_at=?,updated_at=? WHERE task_id=? AND status='suspended' AND awaited_consumer_id=?")
            .bind(&now).bind(&now).bind(task_id).bind(consumer_id)
            .execute(self.pool()).await?.rows_affected();
        if n != 0 {
            self.domain_event_notify().notify_waiters();
        }
        Ok(n)
    }

    /// Suspended steps whose deadline passed, or whose Task left the status
    /// entry they wait in, become `pending`: the first looks at its check
    /// again, the second is superseded by the ordinary entry fence when it
    /// is claimed. Runs ahead of every claim; reads only the (small) partial
    /// index of suspended rows unless one is due.
    async fn wake_due_suspended(&self, now: &str) -> Result<()> {
        const DUE: &str = "status='suspended' AND (suspended_until IS NULL OR suspended_until<=?1 OR NOT EXISTS(SELECT 1 FROM task t WHERE t.id=task_step.task_id AND t.status=task_step.expected_status AND t.status_epoch=task_step.expected_epoch AND t.deleted_at IS NULL))";
        let due: bool = sqlx::query_scalar(&format!(
            "SELECT EXISTS(SELECT 1 FROM task_step WHERE {DUE})"
        ))
        .bind(now)
        .fetch_one(self.pool())
        .await?;
        if due {
            sqlx::query(&format!("UPDATE task_step SET status='pending',suspended_until=NULL,available_at=?1,updated_at=?1 WHERE {DUE}"))
                .bind(now)
                .execute(self.pool())
                .await?;
        }
        Ok(())
    }

    /// A suspended step of this Task, in its current status entry, awaits a
    /// check consumer that is live and whose answer is not yet delivered
    /// (no delivery step exists: its run is queued, running or retried).
    pub async fn awaited_check_is_open(&self, task_id: &str) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_step s JOIN task t ON t.id=s.task_id JOIN check_consumer c ON c.id=s.awaited_consumer_id WHERE s.task_id=? AND s.status='suspended' AND s.expected_status=t.status AND s.expected_epoch=t.status_epoch AND c.cancelled_at IS NULL AND c.delivery_step_id IS NULL)")
            .bind(task_id)
            .fetch_one(self.pool())
            .await?)
    }

    /// Every open check wait of this Task (see [`Self::awaited_check_is_open`])
    /// is a run queued behind a full machine: no slot was free when the check
    /// worker last tried to admit it. Only a run or check ending elsewhere
    /// frees one. `false` when the Task has no open check wait.
    pub async fn awaited_check_waits_for_slot(&self, task_id: &str) -> Result<bool> {
        const OPEN: &str = "FROM task_step s JOIN task t ON t.id=s.task_id JOIN check_consumer c ON c.id=s.awaited_consumer_id LEFT JOIN check_run r ON r.id=c.run_id WHERE s.task_id=?1 AND s.status='suspended' AND s.expected_status=t.status AND s.expected_epoch=t.status_epoch AND c.cancelled_at IS NULL AND c.delivery_step_id IS NULL";
        const SLOT: &str = "r.state='queued' AND r.capacity_wait_since IS NOT NULL";
        Ok(sqlx::query_scalar(&format!(
            "SELECT EXISTS(SELECT 1 {OPEN}) AND NOT EXISTS(SELECT 1 {OPEN} AND NOT COALESCE({SLOT},0))"
        ))
        .bind(task_id)
        .fetch_one(self.pool())
        .await?)
    }

    /// What the open check waits of this Task wait on, for a report: each
    /// suspended step's consumer and the state of the run it joined.
    pub async fn awaited_check_summary(&self, task_id: &str) -> Result<String> {
        let rows: Vec<String> = sqlx::query_scalar("SELECT json_object('step',s.id,'consumer',c.id,'request_key',c.request_key,'result',c.result_id,'run',c.run_id,'run_state',r.state,'run_lease_until',r.lease_until,'run_updated_at',r.updated_at,'slot_wait_since',r.capacity_wait_since,'machine',r.machine_id) FROM task_step s JOIN check_consumer c ON c.id=s.awaited_consumer_id LEFT JOIN check_run r ON r.id=c.run_id WHERE s.task_id=? AND s.status='suspended' ORDER BY s.seq")
            .bind(task_id)
            .fetch_all(self.pool())
            .await?;
        Ok(rows.join(", "))
    }

    /// Check consumers awaited by steps of this Task that will never read
    /// their result: the step was superseded while suspended.
    pub async fn abandoned_check_waits(&self, task_id: &str) -> Result<Vec<(String, String)>> {
        Ok(sqlx::query_as("SELECT id,awaited_consumer_id FROM task_step WHERE task_id=? AND status='superseded' AND awaited_consumer_id IS NOT NULL")
            .bind(task_id)
            .fetch_all(self.pool())
            .await?)
    }
    /// The step that asked for this check will never read its result: the
    /// consumer is cancelled, so its delivery is stale. A queued run it was
    /// the last to wait for is cancelled in the same transaction (it holds
    /// no slot and nobody claimed it, so there is nothing to stop); a run a
    /// worker already claimed is stopped by that worker's next look.
    pub async fn cancel_abandoned_check_consumer(&self, consumer_id: &str) -> Result<bool> {
        let now = now_rfc3339();
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let withdrawn = sqlx::query("UPDATE check_consumer SET cancelled_at=? WHERE id=? AND cancelled_at IS NULL AND applied_at IS NULL")
            .bind(&now)
            .bind(consumer_id)
            .execute(&mut *tx)
            .await?
            .rows_affected()
            == 1;
        if withdrawn {
            sqlx::query(&format!("{CANCEL_UNWATCHED_QUEUED_RUNS} AND id=(SELECT run_id FROM check_consumer WHERE id=?3)"))
                .bind(&now)
                .bind(&now)
                .bind(consumer_id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(withdrawn)
    }
    /// Hooks steps of this Project that still wait, suspended, for a check
    /// consumer that was cancelled under them: `(task_id, consumer_id)`.
    pub async fn cancelled_project_check_waits(
        &self,
        project_id: &str,
    ) -> Result<Vec<(String, String)>> {
        Ok(sqlx::query_as("SELECT s.task_id,s.awaited_consumer_id FROM task_step s JOIN task t ON t.id=s.task_id JOIN check_consumer c ON c.id=s.awaited_consumer_id WHERE t.project_id=? AND s.status='suspended' AND c.cancelled_at IS NOT NULL ORDER BY s.seq")
            .bind(project_id)
            .fetch_all(self.pool())
            .await?)
    }
    /// Whether the entry of the Task's current status was stopped before its
    /// hooks finished: the hooks step of this status entry is `superseded`
    /// and no other step of the entry finished or still runs. A hold that
    /// reaches a Task while its entry hooks wait does this.
    pub async fn entry_hooks_preempted(&self, task_id: &str) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM task_step s JOIN task t ON t.id=s.task_id WHERE s.task_id=?1 AND s.kind='hooks' AND s.status='superseded' AND s.expected_status=t.status AND s.expected_epoch=t.status_epoch)
                AND NOT EXISTS(SELECT 1 FROM task_step s JOIN task t ON t.id=s.task_id WHERE s.task_id=?1 AND s.kind='hooks' AND s.status<>'superseded' AND s.expected_status=t.status AND s.expected_epoch=t.status_epoch)",
        )
        .bind(task_id)
        .fetch_one(self.pool())
        .await?)
    }
    /// Whether the run of a consumer that was cancelled in the last
    /// `within_seconds` is still being stopped. A request for the same check
    /// would join that run and receive its cancellation.
    pub async fn cancelled_consumer_run_is_stopping(
        &self,
        consumer_id: &str,
        within_seconds: i64,
    ) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM check_consumer c JOIN check_run r ON r.id=c.run_id WHERE c.id=? AND c.cancelled_at IS NOT NULL AND (julianday('now')-julianday(c.cancelled_at))*86400.0<? AND r.state IN ('cancelling','cleaning','uncertain'))")
            .bind(consumer_id)
            .bind(within_seconds)
            .fetch_one(self.pool())
            .await?)
    }
    /// Project deletion: nobody waits for this Project's unfinished check
    /// runs any more. Its queued runs are cancelled in the same transaction;
    /// the check worker stops a claimed one on its owner.
    pub async fn cancel_project_check_consumers(&self, project_id: &str) -> Result<u64> {
        let now = now_rfc3339();
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let withdrawn = sqlx::query("UPDATE check_consumer SET cancelled_at=? WHERE cancelled_at IS NULL AND applied_at IS NULL AND run_id IN (SELECT id FROM check_run WHERE project_id=? AND state IN ('queued','running','cancelling','cleaning','uncertain'))")
            .bind(&now)
            .bind(project_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        sqlx::query(&format!("{CANCEL_UNWATCHED_QUEUED_RUNS} AND project_id=?3"))
            .bind(&now)
            .bind(&now)
            .bind(project_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(withdrawn)
    }
    /// The Project's check runs that have not settled: `(id, version, state)`.
    pub async fn live_project_check_runs(
        &self,
        project_id: &str,
    ) -> Result<Vec<(String, i64, String)>> {
        Ok(sqlx::query_as("SELECT id,version,state FROM check_run WHERE project_id=? AND state IN ('queued','running','cancelling','cleaning','uncertain') ORDER BY created_at,id")
            .bind(project_id)
            .fetch_all(self.pool())
            .await?)
    }
    /// The abandoned wait of `step_id` was cleaned up.
    pub async fn forget_check_wait(&self, step_id: &str) -> Result<()> {
        sqlx::query("UPDATE task_step SET awaited_consumer_id=NULL WHERE id=? AND status NOT IN ('suspended','pending','claimed')")
            .bind(step_id)
            .execute(self.pool())
            .await?;
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
    /// One recorded effect of any hook of this step (the newest checkpoint
    /// that has it). The step reads what one of its hooks recorded before
    /// the step was suspended.
    pub async fn step_hook_effect(&self, step_id: &str, key: &str) -> Result<Option<String>> {
        Ok(sqlx::query_scalar("SELECT json_extract(effects_json,?1) FROM task_hook_checkpoint WHERE step_id=?2 AND json_extract(effects_json,?1) IS NOT NULL ORDER BY hook_index DESC LIMIT 1")
            .bind(format!("$.{key}")).bind(step_id).fetch_optional(self.pool()).await?.flatten())
    }
    /// The hooks payload of the Task's current entry, including settled steps.
    /// An entry retry carries its original admission context into the next step.
    pub async fn current_entry_hook_payload(&self, task_id: &str) -> Result<Option<String>> {
        Ok(sqlx::query_scalar("SELECT s.payload_json FROM task_step s JOIN task t ON t.id=s.task_id WHERE s.task_id=? AND s.kind='hooks' AND s.expected_status=t.status AND s.expected_epoch=t.status_epoch ORDER BY s.seq DESC LIMIT 1")
            .bind(task_id).fetch_optional(self.pool()).await?)
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
    /// Enqueue an `integration` step that a later owner command can neither
    /// overtake nor supersede: identity-fenced and marked as started
    /// integration from the moment it exists. The `settle` step calls this in
    /// the transaction that writes the permit, so the protected `result` step
    /// and the permit commit together. Idempotent on the causation key.
    pub async fn enqueue_protected_integration_step_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        input: &EnqueueTaskStep,
    ) -> Result<String> {
        if input.kind != "integration" {
            return Err(DbError::Check(
                "only an integration step is protected at enqueue".into(),
            ));
        }
        let id = self.enqueue_step_in_tx(tx, input).await?;
        sqlx::query("UPDATE task_step SET entry_fenced=0,integration_started_at=COALESCE(integration_started_at,?) WHERE id=? AND status='pending'")
            .bind(now_rfc3339())
            .bind(&id)
            .execute(&mut **tx)
            .await?;
        Ok(id)
    }
    /// Make a pending `integration` step of this Task runnable now, by its
    /// causation key. `false`: no such pending step (not an error).
    pub async fn ready_integration_step(&self, task_id: &str, causation_key: &str) -> Result<bool> {
        let now = now_rfc3339();
        let n = sqlx::query("UPDATE task_step SET available_at=?,updated_at=? WHERE task_id=? AND causation_key=? AND kind='integration' AND status='pending' AND available_at>?")
            .bind(&now)
            .bind(&now)
            .bind(task_id)
            .bind(causation_key)
            .bind(&now)
            .execute(self.pool())
            .await?
            .rows_affected();
        self.domain_event_notify().notify_waiters();
        Ok(n == 1)
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
        self.wake_due_suspended(&now).await?;
        if let Some(lane) = lane {
            let live:i64=sqlx::query_scalar("SELECT COUNT(*) FROM task_step WHERE status='claimed' AND lane=? AND lease_until>?")
                .bind(lane).bind(&now).fetch_one(self.pool()).await?;
            if live >= if lane == "long" { 4 } else { 8 } {
                return Ok(None);
            }
        }
        // Order within a Task: a protected integration step goes before
        // every later step; a higher priority goes first; otherwise `seq`.
        // A higher-priority step that itself waits behind a protected step
        // does not hold back an earlier step that protected step waits for
        // (same priority, earlier `seq`): the three would wait on each other
        // for ever (seen on an upgraded install: a pending cascade, then the
        // worker's protected `result`, then the import's priority-2 cascade).
        let candidate = "SELECT s.* FROM task_step s WHERE s.status IN ('pending','claimed') AND (? IS NULL OR s.lane=?) AND s.task_id NOT IN (SELECT value FROM json_each(?)) AND (? IS NULL OR s.task_id = ?) AND ((s.status = 'pending' AND s.available_at <= ?) OR (s.status = 'claimed' AND s.lease_until <= ?)) AND NOT EXISTS (SELECT 1 FROM task_step p WHERE p.task_id = s.task_id AND ((p.integration_started_at IS NOT NULL AND p.seq<s.seq) OR (s.integration_started_at IS NULL AND p.priority>s.priority AND NOT EXISTS (SELECT 1 FROM task_step q WHERE q.task_id = s.task_id AND q.status IN ('pending','claimed') AND q.integration_started_at IS NOT NULL AND q.priority=s.priority AND q.seq>s.seq AND q.seq<p.seq)) OR (p.priority=s.priority AND p.seq<s.seq)) AND p.status IN ('pending','claimed')) AND NOT EXISTS (SELECT 1 FROM task_step p WHERE p.task_id = s.task_id AND p.id != s.id AND p.lease_until > ?) ORDER BY s.priority DESC,s.created_at,s.seq LIMIT 1";
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

impl SqliteDb {
    pub async fn pending_and_claimed_task_step_counts(
        &self,
    ) -> crate::Result<(i64, i64, Option<String>)> {
        let result = sqlx::query_as("SELECT COALESCE(SUM(status='pending'),0),COALESCE(SUM(status='claimed'),0),MIN(CASE WHEN status='pending' THEN created_at END) FROM task_step WHERE status IN ('pending','claimed')")
            .fetch_one(self.pool()).await?;
        Ok(result)
    }
}

impl SqliteDb {
    pub async fn current_failed_and_parked_task_step_counts(&self) -> crate::Result<(i64, i64)> {
        let result = sqlx::query_as("SELECT COALESCE(SUM(s.status='failed'),0),COALESCE(SUM(s.status='parked'),0) FROM task_step s JOIN task t ON t.id=s.task_id WHERE s.status IN ('failed','parked') AND t.deleted_at IS NULL AND json_extract(t.condition_json,'$.evidence.presentation.failed_step_id')=s.id")
            .fetch_one(self.pool()).await?;
        Ok(result)
    }
}

impl SqliteDb {
    pub async fn integration_task_entry_matches(
        &self,
        task_id: &str,
        status: &str,
        epoch: i64,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar(crate::STEP_FENCE)
            .bind(task_id)
            .bind(status)
            .bind(epoch)
            .fetch_one(self.pool())
            .await?)
    }
}

impl SqliteDb {
    pub async fn integration_preempting_owner_command_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        own_step_id: &Option<String>,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_step WHERE task_id=? AND priority>=1 AND status IN ('pending','claimed') AND (? IS NULL OR id<>?))").bind(task_id).bind(own_step_id).bind(own_step_id).fetch_one(connection).await?)
    }
}

impl SqliteDb {
    pub async fn integration_continuation_exists_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        causation_key: &str,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM task_step WHERE task_id=? AND causation_key=?)",
        )
        .bind(task_id)
        .bind(causation_key)
        .fetch_one(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn integration_suspended_prior_entry_hooks(
        &self,
        task_id: &str,
        sequence: i64,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM task_step h JOIN task t ON t.id=h.task_id \
             WHERE h.task_id=? AND h.kind='hooks' AND h.status='suspended' AND h.seq<? \
               AND h.expected_status=t.status AND h.expected_epoch=t.status_epoch)",
        )
        .bind(task_id)
        .bind(sequence)
        .fetch_one(self.pool())
        .await?)
    }
}

pub async fn integration_task_entry_matches_in_tx(
    connection: &mut sqlx::SqliteConnection,
    task_id: &str,
    status: &str,
    epoch: i64,
) -> crate::Result<bool> {
    Ok(sqlx::query_scalar(crate::STEP_FENCE)
        .bind(task_id)
        .bind(status)
        .bind(epoch)
        .fetch_one(connection)
        .await?)
}

impl SqliteDb {
    pub async fn prioritize_applied_integration_cascade_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        step_id: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE task_step SET priority=2 WHERE id=?")
            .bind(step_id)
            .execute(connection)
            .await?)
    }
}

impl SqliteDb {
    pub async fn integration_step_causation_status(
        &self,
        task_id: &str,
        causation_key: String,
    ) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT status FROM task_step WHERE task_id=? AND causation_key=?")
                .bind(task_id)
                .bind(causation_key)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn task_has_completed_integration_step(&self, task_id: &str) -> crate::Result<bool> {
        Ok(sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM task_step WHERE task_id=? AND integration_started_at IS NOT NULL AND status='done')")
            .bind(task_id)
            .fetch_one(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn task_has_preemptible_work(&self, task_id: &str) -> crate::Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_step WHERE task_id=? AND status IN ('pending','claimed','suspended') AND (kind='hooks' OR (kind='command' AND json_extract(payload_json,'$.operation') IN ('start_execution','claim_task','claim_and_start_task','launch_execution','dispatch_initial_role_execution_with_metadata_and_admission','dispatch_initial_role_execution_with_optional_admission','dispatch_recovery_role','follow_up_interactive_execution','re_execute_execution_with_context'))))")
            .bind(task_id)
            .fetch_one(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn task_step_head_lane(&self, task_id: &str) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>("SELECT lane FROM task_step WHERE task_id=? AND status IN ('pending','claimed') ORDER BY CASE WHEN integration_started_at IS NOT NULL THEN 2 ELSE priority END DESC,seq LIMIT 1")
            .bind(task_id)
            .fetch_optional(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn task_step_integration_started(&self, step_id: &str) -> crate::Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT integration_started_at IS NOT NULL FROM task_step WHERE id=?",
        )
        .bind(step_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn task_step_preemption_requested(&self, step_id: &str) -> crate::Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT preempt_requested_at IS NOT NULL FROM task_step WHERE id=?")
                .bind(step_id)
                .fetch_one(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn supersede_failed_remote_task_step(
        &self,
        last_error: &str,
        updated_at: String,
        step_id: String,
        task_id: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE task_step SET status='superseded',last_error=?,updated_at=? WHERE id=? AND task_id=? AND status IN ('failed','parked')")
            .bind(last_error)
            .bind(updated_at)
            .bind(step_id)
            .bind(task_id)
            .execute(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn store_task_step_command_result_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        result_json: &str,
        step_id: &str,
        claimed_by: &Option<String>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(
            sqlx::query("UPDATE task_step SET result_json=? WHERE id=? AND claimed_by=?")
                .bind(result_json)
                .bind(step_id)
                .bind(claimed_by)
                .execute(connection)
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn task_hook_step_merge_done_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        step_id: &str,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_hook_checkpoint WHERE step_id=? AND json_type(json_extract(effects_json,'$.merge_outcome'),'$.Done') IS NOT NULL)")
            .bind(step_id)
            .fetch_one(connection).await?)
    }
}

impl SqliteDb {
    pub async fn prioritize_merged_follow_up_step_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        step_id: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE task_step SET priority=2 WHERE id=?")
            .bind(step_id)
            .execute(connection)
            .await?)
    }
}

impl SqliteDb {
    pub async fn workflow_transition_pending_steps_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT COUNT(*) FROM task_step WHERE task_id=? AND status IN ('pending','claimed')",
        )
        .bind(task_id)
        .fetch_one(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn record_terminal_cleanup_hook_result_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        result_json: String,
        step_id: &str,
        hook_index: i64,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "UPDATE task_hook_checkpoint SET result_json=? WHERE step_id=? AND hook_index=?",
        )
        .bind(result_json)
        .bind(step_id)
        .bind(hook_index)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn hook_checkpoint_execution_id(
        &self,
        step_id: &str,
        hook_index: i64,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT execution_id FROM task_hook_checkpoint WHERE step_id=? AND hook_index=?",
        )
        .bind(step_id)
        .bind(hook_index)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn claim_run_ci_helper_step(
        &self,
        lease_until: String,
        step_id: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
        "UPDATE task_step SET status='claimed',claimed_by='run-ci-steps',lease_until=? WHERE id=?",
    ).bind(lease_until).bind(step_id).execute(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn remove_run_ci_helper_steps(
        &self,
        task_id: &str,
        step_id: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("DELETE FROM task_step WHERE task_id=? AND (id=? OR (kind='command' AND json_extract(payload_json,'$.operation') IN ('apply_check_result','apply_check_progress')))").bind(task_id).bind(step_id).execute(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn ended_entry_cascade_step_count(
        &self,
        task_id: &str,
        expected_epoch: i64,
        prefix: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar(
                    "SELECT COUNT(*) FROM task_step WHERE task_id=? AND expected_epoch=? AND status IN ('failed','parked','superseded') AND substr(causation_key,1,length(?))=?",
                ).bind(task_id).bind(expected_epoch).bind(prefix).bind(prefix).fetch_one(self.pool()).await?)
    }
}

#[cfg(test)]
mod tests;
