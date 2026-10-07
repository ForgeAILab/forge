//! The task list's page and decorations share a deferred WAL read snapshot.
use super::*;

/// Owns a pooled connection until the request snapshot is dropped. In-memory
/// SQLite pools have one connection, so callers must finish/drop this read
/// before starting another database operation on that pool.
pub struct TaskListRead {
    transaction: sqlx::Transaction<'static, sqlx::Sqlite>,
    pub board_revision: i64,
    pub list_revision: i64,
    pub workflow_definition: String,
    pub project_settings: String,
    pub conditional_safe: bool,
}

impl SqliteDb {
    pub async fn begin_task_list_read(&self, project_id: &str) -> Result<TaskListRead> {
        let mut transaction = self.pool.begin().await?;
        // The first SELECT fixes the snapshot. Conditional hits use this indexed row and the deferred-dispatch index.
        let row = sqlx::query(
            "SELECT board_revision, list_revision, workflow_definition, settings FROM project WHERE id = ?",
        )
        .bind(project_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        Ok(TaskListRead {
            board_revision: row.try_get("board_revision")?,
            workflow_definition: row.try_get("workflow_definition")?,
            project_settings: row.try_get("settings")?,
            list_revision: row.try_get("list_revision")?,
            conditional_safe: !sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (SELECT 1 FROM task WHERE project_id = ? AND json_extract(condition_json,'$.evidence.presentation.retry_recorded')=1 AND deleted_at IS NULL)",
            ).bind(project_id).fetch_one(&mut *transaction).await?,
            transaction,
        })
    }
}

impl TaskListRead {
    pub async fn budgets(
        &mut self,
        ids: &[&str],
    ) -> Result<std::collections::HashMap<String, std::collections::HashMap<String, i64>>> {
        let ids = serde_json::to_string(ids).map_err(|e| DbError::Check(e.to_string()))?;
        let rows=sqlx::query("SELECT task_id,kind,spent FROM task_budget WHERE task_id IN (SELECT value FROM json_each(?))").bind(ids).fetch_all(&mut *self.transaction).await?;
        let mut result =
            std::collections::HashMap::<String, std::collections::HashMap<String, i64>>::new();
        for r in rows {
            result
                .entry(r.try_get("task_id")?)
                .or_default()
                .insert(r.try_get("kind")?, r.try_get("spent")?);
        }
        Ok(result)
    }
    pub async fn list(&mut self, query: TaskListQuery) -> Result<Page<Task>> {
        task::list_page(&mut self.transaction, query).await
    }
    pub async fn reviews(&mut self, ids: &[&str]) -> Result<Vec<Review>> {
        review::latest_reviews(&mut self.transaction, ids).await
    }
    pub async fn executions(
        &mut self,
        queries: &[crate::TaskExecutionProjectionQuery],
    ) -> Result<Vec<Execution>> {
        execution::projection_executions(&mut self.transaction, queries).await
    }
    pub async fn roles(&mut self, ids: &[&str]) -> Result<Vec<crate::TaskRoleAssignment>> {
        workflow::roles_for_tasks(&mut self.transaction, ids).await
    }
    pub async fn transitions(&mut self, ids: &[&str]) -> Result<Vec<crate::TransitionLog>> {
        workflow::transitions_for_tasks(&mut self.transaction, ids).await
    }
    pub async fn links(&mut self, ids: &[&str]) -> Result<Vec<TaskExternalLink>> {
        external_link::latest_links(&mut self.transaction, ids).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TaskBoardRepo;

    #[tokio::test]
    async fn board_move_succeeds_while_execution_is_running_and_heartbeating() {
        let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
        crate::run_migrations(&pool).await.unwrap();
        let db = SqliteDb::new(pool);
        sqlx::query("INSERT INTO project (id, name, created_at, updated_at) VALUES ('heartbeat-project', 'Heartbeat', 'now', 'now')").execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO task (id, project_id, title, status, board_position, created_at, updated_at) VALUES ('first', 'heartbeat-project', 'First', 'todo', 1, 'now', 'now'), ('moved', 'heartbeat-project', 'Moved', 'todo', 2, 'now', 'now')").execute(db.pool()).await.unwrap();
        let revision = TaskBoardRepo::board_revision(&db, "heartbeat-project")
            .await
            .unwrap();
        sqlx::query("INSERT INTO execution (id, task_id, role, status, lease_owner, lease_expires_at, created_at, updated_at) VALUES ('heartbeat-execution', 'moved', 'coder', 'running', 'reader', '2099-01-01T00:00:00Z', 'now', 'now')").execute(db.pool()).await.unwrap();
        let renewed = ExecutionRepo::renew_lease(
            &db,
            crate::RenewExecutionLease {
                execution_id: "heartbeat-execution".to_owned(),
                expected_version: 1,
                owner: "reader".to_owned(),
                lease_expires_at: "2099-01-02T00:00:00Z".to_owned(),
                now: "2026-10-01T00:00:20Z".to_owned(),
            },
        )
        .await
        .unwrap();
        assert!(matches!(renewed, crate::ExecutionLeaseMutation::Updated(_)));
        let input = crate::CompareAndMoveTask {
            post_commit_step: None,
            operation_id: new_uuid_v4(),
            project_id: "heartbeat-project".to_owned(),
            task_id: "moved".to_owned(),
            task_version: 1,
            board_revision: revision,
            target_status: "todo".to_owned(),
            target_column_statuses: vec!["todo".to_owned()],
            before_id: None,
            after_id: Some("first".to_owned()),
            entry_barrier_json: None,
            transition_log_id: new_uuid_v4(),
            workflow_snapshot: serde_json::Value::Null,
            trigger_name: None,
            triggered_by: api_types::Actor::user(api_types::UserActionSource::BoardDrag),
            bridge: Default::default(),
            trigger_reason: "board reorder".to_owned(),
            rejection: false,
            expected_project_version: None,
            expected_workflow_definition: None,
            updated_at: crate::now_rfc3339(),
        };
        let moved = TaskBoardRepo::compare_and_move_task(&db, input)
            .await
            .unwrap();
        assert!(matches!(
            moved,
            crate::MoveTaskPersistence::Committed { .. }
        ));
    }

    #[tokio::test]
    async fn task_list_read_keeps_a_wal_snapshot_across_decorations() {
        let path = std::env::temp_dir().join(format!("task-list-snapshot-{}.db", new_uuid_v4()));
        let pool = crate::create_sqlite_pool(&format!("sqlite:{}", path.display()))
            .await
            .unwrap();
        crate::run_migrations(&pool).await.unwrap();
        let db = SqliteDb::new(pool);
        sqlx::query("INSERT INTO project (id, name, created_at, updated_at) VALUES ('snapshot-project', 'Snapshot', 'now', 'now')").execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO task (id, project_id, title, status, created_at, updated_at) VALUES ('snapshot-task', 'snapshot-project', 'Before', 'todo', 'now', 'now')").execute(db.pool()).await.unwrap();
        let mut snapshot = db.begin_task_list_read("snapshot-project").await.unwrap();
        let revision = snapshot.list_revision;
        sqlx::query("UPDATE task SET title = 'After' WHERE id = 'snapshot-task'")
            .execute(db.pool())
            .await
            .unwrap();
        let query = TaskListQuery {
            project_id: "snapshot-project".to_owned(),
            q: None,
            statuses: vec![],
            agent_ids: vec![],
            assignee_types: vec![],
            assignee_ids: vec![],
            priority: None,
            include_archived: false,
            include_cancelled: false,
            include_deleted: false,
            page: PageRequest {
                cursor: None,
                limit: 20,
                include_total: false,
                sort_by: SortBy::BoardPosition,
                sort_order: SortOrder::Asc,
            },
        };
        assert_eq!(snapshot.list(query).await.unwrap().items[0].title, "Before");
        assert_eq!(snapshot.list_revision, revision);
        drop(snapshot);
        assert!(
            db.begin_task_list_read("snapshot-project")
                .await
                .unwrap()
                .list_revision
                > revision
        );
        db.pool().close().await;
        std::fs::remove_file(path).unwrap();
    }
}
