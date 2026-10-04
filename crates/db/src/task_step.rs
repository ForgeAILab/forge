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
const PRUNE_SETTLED: &str = "DELETE FROM task_step WHERE id IN (SELECT id FROM task_step WHERE status IN ('done','superseded') AND completed_at<? AND (lease_until IS NULL OR lease_until<?) AND task_id NOT IN (SELECT value FROM json_each(?)) AND NOT EXISTS(SELECT 1 FROM task_step live WHERE live.chain_id=task_step.chain_id AND live.status IN ('pending','claimed')) ORDER BY completed_at LIMIT ?)";
const PRUNE_UNRESOLVED: &str = "DELETE FROM task_step WHERE id IN (SELECT id FROM task_step WHERE status IN ('failed','parked') AND completed_at<? AND (lease_until IS NULL OR lease_until<?) AND task_id NOT IN (SELECT value FROM json_each(?)) AND NOT EXISTS(SELECT 1 FROM task_step live WHERE live.chain_id=task_step.chain_id AND live.status IN ('pending','claimed')) ORDER BY completed_at LIMIT ?)";
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
}

#[derive(Debug, Clone)]
pub struct EnqueueTaskStep {
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
    /// Producer reservation. Release after inline hooks and wrapper writes;
    /// a lost producer becomes eligible after this deadline on restart.
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
    async fn renew_step_reservation(&self, id: &str, until: &str) -> Result<bool>;
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
        sqlx::query("INSERT INTO task_step (id,task_id,seq,kind,payload_json,causation_step_id,causation_key,chain_id,chain_position,expected_status,expected_version,expected_epoch,lane,status,available_at,created_at,updated_at) SELECT ?,?,COALESCE(MAX(seq),0)+1,'cascade',?,?,?,?,?,?,?,COALESCE(?,(SELECT status_epoch FROM task WHERE id=?),0),?,'pending',?,?,? FROM task_step WHERE task_id = ? ON CONFLICT(task_id,causation_key) DO NOTHING")
            .bind(&i.id).bind(&i.task_id).bind(&i.payload_json).bind(&i.causation_step_id).bind(&i.causation_key)
            .bind(&i.chain_id).bind(i.chain_position).bind(&i.expected_status).bind(i.expected_version)
            .bind(i.expected_epoch).bind(&i.task_id).bind(&i.lane).bind(&i.available_at).bind(&now).bind(&now).bind(&i.task_id).execute(&mut **tx).await?;
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
        Ok(sqlx::query_scalar(
            "SELECT COUNT(*) FROM task_step WHERE task_id = ? AND status IN ('pending','claimed')",
        )
        .bind(task_id)
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
        Ok(())
    }
    async fn release_step(&self, id: &str, owner: &str) -> Result<()> {
        sqlx::query("UPDATE task_step SET lease_until=NULL,claimed_by=NULL WHERE id=? AND claimed_by=? AND status NOT IN ('pending','claimed')")
            .bind(id).bind(owner).execute(self.pool()).await?;
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
    async fn renew_step_reservation(&self, id: &str, until: &str) -> Result<bool> {
        Ok(
            sqlx::query("UPDATE task_step SET available_at=? WHERE id=? AND status='pending'")
                .bind(until)
                .bind(id)
                .execute(self.pool())
                .await?
                .rows_affected()
                == 1,
        )
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
}
impl Drop for TaskStepActivity {
    fn drop(&mut self) {
        self.state.lock().expect("step activity").remove(&self.id);
    }
}
impl SqliteDb {
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
    /// step, a claimed fast-lane step, and a done step still holding its lease
    /// for the inline dispatch that follows its CAS. Steps behind another
    /// step, in back-off, under a producer reservation, or waiting for or
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
                   AND NOT EXISTS(SELECT 1 FROM task_step p WHERE p.task_id=s.task_id AND p.id!=s.id AND p.lease_until>?)) \
               OR (s.status='claimed' AND s.lane='fast' AND s.lease_until>?) \
               OR (s.status='done' AND s.lease_until>?)) \
             AND NOT EXISTS(SELECT 1 FROM execution e WHERE e.task_id=s.task_id AND e.status='running')",
        )
        .bind(agent_id)
        .bind(excluding_task)
        .bind(&now)
        .bind(&now)
        .bind(&now)
        .bind(&now)
        .fetch_one(self.pool())
        .await?)
    }
    pub fn active_step_count(&self) -> usize {
        self.task_step_activity.lock().expect("step activity").len()
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
        let candidate = "SELECT s.* FROM task_step s WHERE s.status IN ('pending','claimed') AND (? IS NULL OR s.lane=?) AND s.task_id NOT IN (SELECT value FROM json_each(?)) AND (? IS NULL OR s.task_id = ?) AND ((s.status = 'pending' AND s.available_at <= ?) OR (s.status = 'claimed' AND s.lease_until <= ?)) AND NOT EXISTS (SELECT 1 FROM task_step p WHERE p.task_id = s.task_id AND p.seq < s.seq AND p.status IN ('pending','claimed')) AND NOT EXISTS (SELECT 1 FROM task_step p WHERE p.task_id = s.task_id AND p.id != s.id AND p.lease_until > ?) ORDER BY s.created_at,s.seq LIMIT 1";
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
        assert_eq!(db.queued_admissions("agent", "z").await.unwrap(), (3, 3));
        assert_eq!(db.queued_admissions("other", "a").await.unwrap(), (0, 2));
    }
}
