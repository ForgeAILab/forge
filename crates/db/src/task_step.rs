//! Task cascade outbox; all ordering and ownership decisions are transactional.
use crate::{begin_immediate, now_rfc3339, DbError, Result, SqliteDb};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::{Row, Sqlite, Transaction};

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
    pub producing_transition_id: Option<String>,
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
    pub producing_transition_id: Option<String>,
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
    async fn prune_steps(&self, before: &str, limit: i64) -> Result<u64>;
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
        producing_transition_id: row.get("producing_transition_id"),
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
        sqlx::query("INSERT INTO task_step (id,task_id,seq,kind,payload_json,causation_step_id,causation_key,chain_id,chain_position,expected_status,expected_version,producing_transition_id,lane,status,available_at,created_at,updated_at) SELECT ?,?,COALESCE(MAX(seq),0)+1,'cascade',?,?,?,?,?,?,?,?,?,'pending',?,?,? FROM task_step WHERE task_id = ? ON CONFLICT(task_id,causation_key) DO NOTHING")
            .bind(&i.id).bind(&i.task_id).bind(&i.payload_json).bind(&i.causation_step_id).bind(&i.causation_key)
            .bind(&i.chain_id).bind(i.chain_position).bind(&i.expected_status).bind(i.expected_version)
            .bind(&i.producing_transition_id).bind(&i.lane).bind(&i.available_at).bind(&now).bind(&now).bind(&i.task_id).execute(&mut **tx).await?;
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
    async fn prune_steps(&self, before: &str, limit: i64) -> Result<u64> {
        let active: Vec<String> = self
            .task_step_activity
            .lock()
            .expect("step activity")
            .values()
            .map(|(id, _)| id.clone())
            .collect();
        let active = serde_json::to_string(&active)
            .map_err(|e| DbError::from(sqlx::Error::Decode(Box::new(e))))?;
        let deleted=sqlx::query("DELETE FROM task_step WHERE id IN (SELECT id FROM task_step WHERE status IN ('done','superseded') AND completed_at<? AND (lease_until IS NULL OR lease_until<?) AND task_id NOT IN (SELECT value FROM json_each(?)) AND NOT EXISTS(SELECT 1 FROM task_step live WHERE live.chain_id=task_step.chain_id AND live.status IN ('pending','claimed')) ORDER BY completed_at LIMIT ?)")
            .bind(before).bind(now_rfc3339()).bind(active).bind(limit.clamp(1,100)).execute(self.pool()).await?.rows_affected();
        sqlx::query("DELETE FROM task_step_workflow WHERE id IN (SELECT id FROM task_step_workflow WHERE last_used_at<? AND NOT EXISTS(SELECT 1 FROM task_step s WHERE json_extract(s.payload_json,'$.workflow_ref.id')=task_step_workflow.id) LIMIT ?)")
            .bind(before).bind(limit.clamp(1,100)).execute(self.pool()).await?;
        Ok(deleted)
    }
    async fn step_entry_matches(&self, step: &TaskStep) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task WHERE id=? AND status=? AND deleted_at IS NULL AND (SELECT id FROM transition_log WHERE task_id=task.id AND is_status_entry=1 ORDER BY created_at DESC,id DESC LIMIT 1) IS ?)")
            .bind(&step.task_id).bind(&step.expected_status).bind(&step.producing_transition_id).fetch_one(self.pool()).await?)
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
        sqlx::query("UPDATE task_step SET status='pending',claimed_by=NULL,lease_until=NULL,last_error=?,available_at=?,updated_at=? WHERE id=? AND status='claimed' AND claimed_by=? AND lease_until > ?")
            .bind(error).bind(available_at).bind(now_rfc3339()).bind(&s.id).bind(&s.claimed_by).bind(now_rfc3339())
            .execute(self.pool()).await?;
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
            producing_transition_id: None,
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
    async fn entry_identity_tolerates_versions_but_not_a_return_to_same_status() {
        let db = fixture().await;
        let id = db.enqueue_step(&input("a", "edit")).await.unwrap();
        let step = db
            .task_steps("a")
            .await
            .unwrap()
            .into_iter()
            .find(|s| s.id == id)
            .unwrap();
        sqlx::query("UPDATE task SET title='edited',version=version+1 WHERE id='a'")
            .execute(db.pool())
            .await
            .unwrap();
        assert!(db.step_entry_matches(&step).await.unwrap());
        sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,created_at) VALUES('new-entry','a','planning','todo','user','returned',?)").bind(now_rfc3339()).execute(db.pool()).await.unwrap();
        assert!(!db.step_entry_matches(&step).await.unwrap());
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
            db.prune_steps("2020-01-01T00:00:00Z", 1000).await.unwrap(),
            100
        );
        assert_eq!(db.task_steps("a").await.unwrap().len(), 5);
        assert_eq!(db.pending_steps("b").await.unwrap(), 1);
    }
}
