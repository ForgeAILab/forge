//! Batched scheduler snapshots and generation-fenced dirty consumption.
use super::*;
use crate::{TaskCondition, TaskRoleAssignment, TransitionLog};
use std::collections::HashMap;

#[derive(Debug)]
pub struct ScheduleRead {
    pub task: Task,
    pub epoch: i64,
    pub condition: TaskCondition,
    pub generation: i64,
    pub dirty: bool,
    pub external: bool,
    pub assignments: Vec<TaskRoleAssignment>,
    pub executions: Vec<Execution>,
    pub reviews: Vec<Review>,
    pub transitions: Vec<TransitionLog>,
    pub children: Vec<Task>,
    pub parent: Option<Task>,
    pub siblings: Vec<Task>,
    pub queue_owned: bool,
    pub entry_hooks_seen: bool,
    pub park_json: Option<String>,
    pub has_wait: bool,
    pub capability_class: Option<String>,
    pub placement_state: Option<String>,
    pub placement_daemon_id: Option<String>,
    pub ready_environment_digest: Option<String>,
}
impl SqliteDb {
    pub fn schedule_commit_pending(&self) -> bool {
        self.domain_event_hooks.pending_commit()
    }
    pub fn schedule_generation(&self) -> u64 {
        self.domain_event_hooks.generation()
    }
    /// Unordered: the reconciler orders candidates by the Project's workflow.
    pub async fn dirty_schedule_tasks(&self, limit: i64) -> Result<Vec<String>> {
        Ok(
            sqlx::query_scalar("SELECT task_id FROM task_schedule_dirty WHERE dirty=1 LIMIT ?")
                .bind(limit)
                .fetch_all(self.pool())
                .await?,
        )
    }
    /// No generation arriving during reconciliation can be acknowledged away.
    pub async fn acknowledge_schedule(&self, id: &str, generation: i64) -> Result<()> {
        if generation == 0 {
            return Ok(());
        }
        sqlx::query("UPDATE task_schedule_dirty SET dirty=0,external=0 WHERE task_id=? AND generation=? AND dirty=1")
            .bind(id)
            .bind(generation)
            .execute(self.pool())
            .await?;
        Ok(())
    }
    pub async fn kick_schedule(&self, id: &str) -> Result<()> {
        sqlx::query("INSERT INTO task_schedule_dirty(task_id,external) SELECT id,1 FROM task WHERE id=? ON CONFLICT(task_id) DO UPDATE SET generation=generation+1,dirty=1,external=1")
            .bind(id).execute(self.pool()).await?;
        Ok(())
    }
    pub async fn schedule_reads(&self, ids: &[String]) -> Result<Vec<ScheduleRead>> {
        Ok(self.schedule_reads_isolated(ids).await?.0)
    }
    /// The batched snapshot, plus the Tasks whose own row could not be decoded.
    /// One unreadable row never fails the page it shares with healthy Tasks.
    pub async fn schedule_reads_isolated(
        &self,
        ids: &[String],
    ) -> Result<(Vec<ScheduleRead>, Vec<(String, DbError)>)> {
        let mut unreadable = Vec::new();
        if ids.is_empty() {
            return Ok((Vec::new(), unreadable));
        }
        let ids = serde_json::to_string(ids).map_err(|e| DbError::Check(e.to_string()))?;
        let mut reader = self.pool().begin().await?;
        let mut reads = Vec::new();
        for row in sqlx::query("SELECT t.*,COALESCE(d.generation,0) AS dirty_generation,COALESCE(d.dirty,0) AS dirty,COALESCE(d.external,0) AS dirty_external,EXISTS(SELECT 1 FROM task_step s WHERE s.task_id=t.id AND s.kind!='mutation' AND s.status IN ('pending','claimed','suspended') AND (s.entry_fenced=0 OR (s.expected_epoch=t.status_epoch AND s.expected_status=t.status))) AS queue_owned,EXISTS(SELECT 1 FROM task_step s WHERE s.task_id=t.id AND s.kind='hooks' AND s.expected_epoch=t.status_epoch AND s.expected_status=t.status) AS hooks_seen,(SELECT state FROM workspace_placement WHERE task_id=COALESCE(t.parent_task_id,t.id)) AS placement_state,(SELECT COALESCE(execution_daemon_id,daemon_id) FROM workspace_placement WHERE task_id=COALESCE(t.parent_task_id,t.id)) AS placement_daemon_id,(SELECT checks_digest FROM project_machine_readiness WHERE project_id=t.project_id AND status='ready' AND owner_kind=json_extract(CASE WHEN json_valid(t.metadata_json) THEN t.metadata_json ELSE '{}' END,'$.environment_wait.machine.owner_kind') AND daemon_id=COALESCE(json_extract(CASE WHEN json_valid(t.metadata_json) THEN t.metadata_json ELSE '{}' END,'$.environment_wait.machine.daemon_id'),'') AND runtime_id=COALESCE(json_extract(CASE WHEN json_valid(t.metadata_json) THEN t.metadata_json ELSE '{}' END,'$.environment_wait.machine.runtime_id'),'')) AS ready_environment_digest,(SELECT capability_class FROM project_task_governance WHERE task_id=t.id) AS capability_class,(SELECT reason_json FROM task_schedule_park WHERE task_id=t.id AND epoch=t.status_epoch) AS park_json,EXISTS(SELECT 1 FROM task_schedule_wait WHERE task_id=t.id) AS has_wait FROM task t LEFT JOIN task_schedule_dirty d ON d.task_id=t.id WHERE t.id IN (SELECT value FROM json_each(?))")
            .bind(&ids).fetch_all(&mut *reader).await? {
            let id: String = row.try_get("id")?;
            let condition = row
                .try_get::<String, _>("condition_json")
                .map_err(DbError::from)
                .and_then(|raw| {
                    serde_json::from_str(&raw).map_err(|e| DbError::Check(e.to_string()))
                });
            let condition = match condition {
                Ok(condition) => condition,
                Err(error) => {
                    unreadable.push((id, error));
                    continue;
                }
            };
            reads.push(ScheduleRead {
                epoch: row.try_get("status_epoch")?,
                condition,
                generation: row.try_get("dirty_generation")?, dirty:row.try_get("dirty")?, external: row.try_get::<i64,_>("dirty_external")? != 0,
                queue_owned: row.try_get("queue_owned")?, entry_hooks_seen: row.try_get("hooks_seen")?, placement_state: row.try_get("placement_state")?, placement_daemon_id:row.try_get("placement_daemon_id")?, ready_environment_digest:row.try_get("ready_environment_digest")?, capability_class:row.try_get("capability_class")?, park_json:row.try_get("park_json")?, has_wait: row.try_get("has_wait")?,
                task: map_schedule_task(row)?, assignments: Vec::new(), executions: Vec::new(), reviews: Vec::new(), transitions: Vec::new(), children: Vec::new(), parent: None, siblings: Vec::new(),
            });
        }
        let index: HashMap<String, usize> = reads
            .iter()
            .enumerate()
            .map(|(i, r)| (r.task.id.clone(), i))
            .collect();
        let parent_ids: Vec<_> = reads
            .iter()
            .filter_map(|r| r.task.parent_task_id.clone())
            .collect();
        let parent_ids =
            serde_json::to_string(&parent_ids).map_err(|e| DbError::Check(e.to_string()))?;
        let related = format!("SELECT {TASK_COLUMNS} FROM task WHERE id IN (SELECT value FROM json_each(?)) OR parent_task_id IN (SELECT value FROM json_each(?)) OR parent_task_id IN (SELECT value FROM json_each(?))");
        let family: Vec<Task> = sqlx::query(&related)
            .bind(&parent_ids)
            .bind(&parent_ids)
            .bind(&ids)
            .fetch_all(&mut *reader)
            .await?
            .into_iter()
            .map(map_schedule_task)
            .collect::<Result<_>>()?;
        for r in &mut reads {
            r.parent = family
                .iter()
                .find(|t| Some(&t.id) == r.task.parent_task_id.as_ref())
                .cloned();
            r.children = family
                .iter()
                .filter(|t| {
                    t.parent_task_id.as_deref() == Some(&r.task.id) && t.deleted_at.is_none()
                })
                .cloned()
                .collect();
            r.siblings = family
                .iter()
                .filter(|t| {
                    r.task.parent_task_id.is_some()
                        && t.parent_task_id == r.task.parent_task_id
                        && t.deleted_at.is_none()
                })
                .cloned()
                .collect();
            r.children
                .sort_by_key(|t| (t.subtask_order, t.created_at.clone(), t.id.clone()));
            r.siblings
                .sort_by_key(|t| (t.subtask_order, t.created_at.clone(), t.id.clone()));
        }
        for row in sqlx::query("SELECT id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at FROM task_role_assignment WHERE task_id IN (SELECT value FROM json_each(?)) OR task_id IN (SELECT value FROM json_each(?))")
            .bind(&ids).bind(&parent_ids).fetch_all(&mut *reader).await? {
            let a = super::workflow::map_task_role_assignment_row(row)?;
            for r in &mut reads {
                if a.task_id==r.task.id || (a.role_name=="coder" && Some(&a.task_id)==r.task.parent_task_id.as_ref()) { r.assignments.push(a.clone()); }
            }
        }
        // Latest of each role, plus every running execution. Indexed by Task;
        // this never fetches the unbounded execution history into Rust.
        for row in sqlx::query("WITH requested AS (SELECT value AS id FROM json_each(?)), roles AS (SELECT task_id,role FROM execution WHERE task_id IN (SELECT id FROM requested) GROUP BY task_id,role), selected AS (SELECT id FROM execution WHERE task_id IN (SELECT id FROM requested) AND status='running' UNION SELECT (SELECT x.id FROM execution x WHERE x.task_id=roles.task_id AND x.role=roles.role ORDER BY x.created_at DESC,x.id DESC LIMIT 1) FROM roles) SELECT e.* FROM execution e WHERE e.id IN (SELECT id FROM selected) ORDER BY e.created_at,e.id")
            .bind(&ids).fetch_all(&mut *reader).await? {
            let e = map_execution(row)?; if let Some(i)=index.get(&e.task_id) { reads[*i].executions.push(e); }
        }
        for row in sqlx::query("SELECT * FROM review WHERE task_id IN (SELECT value FROM json_each(?)) ORDER BY attempt_number,id")
            .bind(&ids).fetch_all(&mut *reader).await? {
            let r=map_review(row)?; if let Some(i)=index.get(&r.task_id) { reads[*i].reviews.push(r); }
        }
        for row in sqlx::query("SELECT id,task_id,from_state,to_state,trigger_name,triggered_by,trigger_reason,hook_results_json,rejection,created_at,bridge_kind,bridge_payload FROM transition_log WHERE task_id IN (SELECT value FROM json_each(?)) ORDER BY created_at,rowid")
            .bind(&ids).fetch_all(&mut *reader).await? {
            let t=super::workflow::map_transition_log_row(row)?; if let Some(i)=index.get(&t.task_id) { reads[*i].transitions.push(t); }
        }
        reader.rollback().await?;
        Ok((reads, unreadable))
    }
    pub async fn record_schedule_park(&self, id: &str, epoch: i64, reason: &str) -> Result<()> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let before = self
            .get_task_in_tx(&mut tx, id)
            .await?
            .ok_or(DbError::NotFound)?;
        let changed = sqlx::query("INSERT INTO task_schedule_park(task_id,epoch,reason_json) SELECT id,?,? FROM task WHERE id=? AND status_epoch=? ON CONFLICT(task_id) DO UPDATE SET epoch=excluded.epoch,reason_json=excluded.reason_json WHERE epoch IS NOT excluded.epoch OR reason_json IS NOT excluded.reason_json")
            .bind(epoch).bind(reason).bind(id).bind(epoch).execute(&mut *tx).await?.rows_affected() != 0;
        if changed {
            crate::task_condition::produce(&mut tx, id, crate::ConditionChange::Legacy).await?;
            self.append_condition_change_in_tx(&mut tx, &before).await?;
        }
        tx.commit().await?;
        Ok(())
    }
    pub async fn clear_visible_schedule_park(&self, id: &str) -> Result<bool> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let before = self
            .get_task_in_tx(&mut tx, id)
            .await?
            .ok_or(DbError::NotFound)?;
        let raw = sqlx::query_scalar::<_, String>(
            "SELECT reason_json FROM task_schedule_park WHERE task_id=?",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;
        let visible = raw
            .and_then(|s| serde_json::from_str(&s).ok())
            .as_ref()
            .and_then(crate::task_condition::readers::owner_park)
            .is_some();
        if visible {
            sqlx::query("DELETE FROM task_schedule_park WHERE task_id=?")
                .bind(id)
                .execute(&mut *tx)
                .await?;
            crate::task_condition::produce(&mut tx, id, crate::ConditionChange::Legacy).await?;
            self.append_condition_change_in_tx(&mut tx, &before).await?;
        }
        tx.commit().await?;
        Ok(visible)
    }
    async fn append_condition_change_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        before: &Task,
    ) -> Result<()> {
        let after = self
            .get_task_in_tx(tx, &before.id)
            .await?
            .ok_or(DbError::NotFound)?;
        if crate::task_condition::material_blocker(&before.condition)
            != crate::task_condition::material_blocker(&after.condition)
        {
            let event = crate::CreateDomainEvent::task_interruption_changed(&after);
            crate::DomainEventRepo::append_event_in_tx(self, tx, &event).await?;
        }
        Ok(())
    }
}
impl SqliteDb {
    /// `daemon` names the machine the Task waits on (`"*"`: a run slot on
    /// any machine); `project.1` is set only while it waits on the Project limit.
    pub async fn schedule_wait(
        &self,
        id: &str,
        project: (&str, bool),
        agent: Option<&str>,
        daemon: Option<&str>,
        deadline: Option<&str>,
    ) -> Result<()> {
        if agent.is_none() && daemon.is_none() && deadline.is_none() && !project.1 {
            sqlx::query("DELETE FROM task_schedule_wait WHERE task_id=?")
                .bind(id)
                .execute(self.pool())
                .await?;
            return Ok(());
        }
        sqlx::query("INSERT INTO task_schedule_wait(task_id,project_id,agent_id,daemon_id,deadline,project_capacity) VALUES (?,?,?,?,?,?) ON CONFLICT(task_id) DO UPDATE SET project_id=excluded.project_id,agent_id=excluded.agent_id,daemon_id=excluded.daemon_id,deadline=excluded.deadline,project_capacity=excluded.project_capacity WHERE project_capacity IS NOT excluded.project_capacity OR project_id IS NOT excluded.project_id OR agent_id IS NOT excluded.agent_id OR daemon_id IS NOT excluded.daemon_id OR deadline IS NOT excluded.deadline")
            .bind(id).bind(project.0).bind(agent).bind(daemon).bind(deadline).bind(project.1).execute(self.pool()).await?;
        Ok(())
    }
    pub async fn due_schedule_tasks(&self, now: &str) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT task_id FROM task_schedule_wait WHERE julianday(deadline)<=julianday(?1) UNION SELECT task_id FROM task_step WHERE status IN ('pending','claimed','suspended') AND julianday(CASE status WHEN 'claimed' THEN lease_until WHEN 'suspended' THEN suspended_until ELSE available_at END)<=julianday(?1) UNION SELECT task_id FROM execution WHERE status='running' AND julianday(lease_expires_at)<=julianday(?1) UNION SELECT task_id FROM workspace_placement WHERE state IN ('reserved','preparing') AND julianday(reserved_until)<=julianday(?1)",
        )
        .bind(now)
        .fetch_all(self.pool())
        .await?)
    }
    /// The earliest timer any Task waits on: retries, step and execution
    /// leases, placement reservations and readiness rechecks.
    pub async fn next_schedule_deadline(&self) -> Result<Option<String>> {
        Ok(sqlx::query_scalar("SELECT MIN(deadline) FROM (SELECT deadline FROM task_schedule_wait WHERE deadline IS NOT NULL UNION ALL SELECT next_check_at FROM project_machine_readiness WHERE status='not_ready' UNION ALL SELECT reserved_until FROM workspace_placement WHERE state IN ('reserved','preparing') UNION ALL SELECT lease_until FROM task_step WHERE status='claimed' UNION ALL SELECT lease_expires_at FROM execution WHERE status='running') WHERE julianday(deadline)>julianday('now')")
            .fetch_one(self.pool())
            .await?)
    }
    /// One keyset page of Tasks that are not settled, read from the partial
    /// index: a settled Task is never visited.
    pub async fn open_schedule_tasks(
        &self,
        after: Option<&str>,
        limit: i64,
    ) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar("SELECT id FROM task WHERE json_extract(condition_json,'$.kind') != 'settled' AND (?1 IS NULL OR id>?1) ORDER BY id LIMIT ?2")
            .bind(after)
            .bind(limit)
            .fetch_all(self.pool())
            .await?)
    }
    /// Tasks waiting for a machine run slot.
    pub async fn schedule_machine_waiters(&self) -> Result<Vec<String>> {
        Ok(
            sqlx::query_scalar("SELECT task_id FROM task_schedule_wait WHERE daemon_id='*'")
                .fetch_all(self.pool())
                .await?,
        )
    }
    /// Re-read one Task at `deadline`: a transient admission failure is
    /// retried then, without touching the Task's own fields.
    pub async fn schedule_retry_at(&self, id: &str, deadline: &str) -> Result<()> {
        sqlx::query("INSERT INTO task_schedule_wait(task_id,project_id,deadline) SELECT id,project_id,?2 FROM task WHERE id=?1 ON CONFLICT(task_id) DO UPDATE SET deadline=excluded.deadline")
            .bind(id)
            .bind(deadline)
            .execute(self.pool())
            .await?;
        Ok(())
    }
    pub async fn schedule_has_owner(&self, id: &str, epoch: i64) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_step WHERE task_id=? AND status IN ('pending','claimed','suspended') AND (entry_fenced=0 OR expected_epoch=?)) OR EXISTS(SELECT 1 FROM execution WHERE task_id=? AND status='running' AND role!='interactive') OR EXISTS(SELECT 1 FROM task_schedule_park WHERE task_id=? AND epoch=?) OR EXISTS(SELECT 1 FROM task WHERE id=? AND json_extract(condition_json,'$.kind') IN ('parked','failed'))")
            .bind(id).bind(epoch).bind(id).bind(id).bind(epoch).bind(id).fetch_one(self.pool()).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    async fn fixture() -> SqliteDb {
        let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
        crate::run_migrations(&pool).await.unwrap();
        sqlx::raw_sql("INSERT INTO project(id,name,settings,workflow_definition,created_at,updated_at) VALUES ('p','p','{}','{}','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z'); INSERT INTO task(id,project_id,title,task_type,status,created_at,updated_at) VALUES ('root','p','root','task','in_progress','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z'),('child','p','child','task','in_progress','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z'),('dependent','p','dependent','task','todo','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z'),('unrelated','p','unrelated','task','todo','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z'); UPDATE task SET parent_task_id='root',subtask_order=0 WHERE id='child'; INSERT INTO task_dependency VALUES ('dependent','child','2026-10-06T00:00:00Z'); DELETE FROM task_schedule_dirty;").execute(&pool).await.unwrap();
        SqliteDb::new(pool)
    }
    #[tokio::test]
    async fn child_and_dependency_fanout_excludes_unrelated_tasks() {
        let db = fixture().await;
        sqlx::query("UPDATE task SET status='done' WHERE id='child'")
            .execute(db.pool())
            .await
            .unwrap();
        let mut ids = db.dirty_schedule_tasks(100).await.unwrap();
        ids.sort();
        assert_eq!(ids, ["child", "dependent", "root"]);
    }
    #[tokio::test]
    async fn rollback_does_not_kick_and_new_generation_cannot_be_lost() {
        let db = fixture().await;
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        sqlx::query("UPDATE task SET status='done' WHERE id='child'")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        assert!(db.dirty_schedule_tasks(100).await.unwrap().is_empty());
        db.kick_schedule("unrelated").await.unwrap();
        let read = db
            .schedule_reads(&["unrelated".into()])
            .await
            .unwrap()
            .pop()
            .unwrap();
        db.kick_schedule("unrelated").await.unwrap();
        db.acknowledge_schedule("unrelated", read.generation)
            .await
            .unwrap();
        assert_eq!(db.dirty_schedule_tasks(100).await.unwrap(), ["unrelated"]);
    }
    #[tokio::test]
    async fn deadline_is_exact_and_does_not_wait_for_sweep() {
        let db = fixture().await;
        db.schedule_wait(
            "unrelated",
            ("p", false),
            None,
            None,
            Some("2026-10-06T12:00:00Z"),
        )
        .await
        .unwrap();
        assert!(db
            .due_schedule_tasks("2026-10-06T11:59:59Z")
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            db.due_schedule_tasks("2026-10-06T12:00:00Z").await.unwrap(),
            ["unrelated"]
        );
    }
}
impl SqliteDb {
    /// Read-only liveness proof, independent of the dirty notification path.
    pub async fn task_schedule_violations(&self) -> Result<Vec<String>> {
        let tasks:Vec<(String,i64)>=sqlx::query_as("SELECT id,status_epoch FROM task WHERE json_extract(condition_json,'$.kind') != 'settled' AND deleted_at IS NULL AND archived_at IS NULL")
            .fetch_all(self.pool()).await?;
        let mut violations = Vec::new();
        for (id, epoch) in tasks {
            if !self.schedule_has_owner(&id, epoch).await? {
                violations.push(id);
            }
        }
        Ok(violations)
    }
}
impl SqliteDb {
    pub async fn schedule_projects(&self, sweep: bool) -> Result<Vec<(Project, i64)>> {
        let rows=sqlx::query("SELECT p.*,COALESCE(d.generation,0) AS dirty_generation FROM project p LEFT JOIN project_schedule_dirty d ON d.project_id=p.id WHERE ? OR d.project_id IS NOT NULL OR p.system_pause_reason='repository_not_ready' ORDER BY p.created_at,p.id")
            .bind(sweep).fetch_all(self.pool()).await?;
        rows.into_iter()
            .map(|row| {
                let generation = row.try_get("dirty_generation")?;
                Ok((map_project(row)?, generation))
            })
            .collect()
    }
    pub async fn acknowledge_schedule_project(&self, id: &str, generation: i64) -> Result<()> {
        if generation == 0 {
            return Ok(());
        }
        sqlx::query("DELETE FROM project_schedule_dirty WHERE project_id=? AND generation=?")
            .bind(id)
            .bind(generation)
            .execute(self.pool())
            .await?;
        Ok(())
    }
}
impl SqliteDb {
    pub async fn schedule_agents(&self, ids: &[String]) -> Result<Vec<Agent>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids = serde_json::to_string(ids).map_err(|e| DbError::Check(e.to_string()))?;
        sqlx::query("SELECT * FROM agent_current WHERE id IN (SELECT value FROM json_each(?))")
            .bind(ids)
            .fetch_all(self.pool())
            .await?
            .into_iter()
            .map(map_agent)
            .collect()
    }
}

fn map_schedule_task(row: SqliteRow) -> Result<Task> {
    Ok(Task {
        // The same stored condition every other row reader gets. A read that
        // does not project the column is an error, never a silent `Clear`.
        condition: crate::task_condition::decode_or_unknown(row.try_get("condition_json")?),
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        parent_task_id: row.try_get("parent_task_id")?,
        assignee_type: row.try_get("assignee_type")?,
        assignee_id: row.try_get("assignee_id")?,
        title: row.try_get("title")?,
        description: row.try_get("description")?,
        task_type: row.try_get("task_type")?,
        status: row.try_get("status")?,
        is_automation: row.try_get::<i64, _>("is_automation")? != 0,
        priority: row.try_get("priority")?,
        board_position: row.try_get("board_position")?,
        subtask_order: row.try_get("subtask_order")?,
        task_state_config: row.try_get("task_state_config")?,
        merge_config: row.try_get("merge_config")?,
        metadata_json: row.try_get("metadata_json").unwrap_or(None),
        plan: row.try_get("plan")?,
        error_annotation: row.try_get("error_annotation").unwrap_or(None),
        blocked_json: row.try_get("blocked_json").unwrap_or(None),
        failed_json: row.try_get("failed_json").unwrap_or(None),
        entry_barrier_json: row.try_get("entry_barrier_json").unwrap_or(None),
        review_passed_at: row.try_get("review_passed_at")?,
        archived_at: row.try_get("archived_at")?,
        deleted_at: row.try_get("deleted_at")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

#[cfg(test)]
mod fanout_tests {
    use super::*;
    #[tokio::test]
    async fn capacity_release_kicks_only_matching_waiters() {
        let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
        crate::run_migrations(&pool).await.unwrap();
        sqlx::raw_sql("INSERT INTO project(id,name,settings,workflow_definition,created_at,updated_at) VALUES ('p','p','{}','{}','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z'); INSERT INTO task(id,project_id,title,task_type,status,created_at,updated_at) VALUES ('work','p','work','task','in_progress','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z'),('wait','p','wait','task','todo','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z'),('other','p','other','task','todo','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z'),('idle','p','idle','task','todo','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z'); INSERT INTO task_schedule_wait(task_id,project_id,daemon_id) VALUES ('wait','p','*'),('other','p','other-machine'),('idle','p',NULL); INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES ('e','work','coder','running','2026-10-06T00:00:00Z','2026-10-06T00:00:00Z'); DELETE FROM task_schedule_dirty;").execute(&pool).await.unwrap();
        let db = SqliteDb::new(pool);
        sqlx::query("UPDATE execution SET status='completed' WHERE id='e'")
            .execute(db.pool())
            .await
            .unwrap();
        let mut ids = db.dirty_schedule_tasks(100).await.unwrap();
        ids.sort();
        assert_eq!(ids, ["wait", "work"]);
    }
}
