use super::*;

/// Explicit prerequisites and the hierarchy's completion/order waits form
/// one graph of `(Task, phase)` nodes: a Task's start waits on each
/// prerequisite's completion; its completion waits on its own start; a child
/// starts after its parent started; a parent completes after its children;
/// a later sibling starts after the preceding unfinished sibling completed.
/// Settled Tasks contribute no waits. Dependencies and parentage never cross
/// a Project, so one indexed read loads the Task's Project and the search
/// runs in memory, linear in that graph: a reorder checks every child
/// against one load. The writer transaction also fences a concurrent write.
struct WaitGraph {
    index: std::collections::HashMap<(String, bool), usize>,
    edges: Vec<Vec<usize>>,
}

const START: bool = false;
const DONE: bool = true;

impl WaitGraph {
    async fn load(tx: &mut Transaction<'_, Sqlite>, project_of: &str) -> Result<Self> {
        let rows: Vec<(String, bool, String, bool)> = sqlx::query_as(
            "WITH live AS (
                SELECT id,parent_task_id,subtask_order,created_at FROM task
                WHERE project_id=(SELECT project_id FROM task WHERE id=?1)
                  AND deleted_at IS NULL AND json_extract(condition_json,'$.kind') != 'settled'
             ), ordered AS (
                SELECT *,ROW_NUMBER() OVER(PARTITION BY parent_task_id ORDER BY subtask_order,created_at,id) AS position
                FROM live WHERE parent_task_id IS NOT NULL
             )
             SELECT d.task_id,0,d.depends_on_id,1 FROM task_dependency d
               JOIN live t ON t.id=d.task_id JOIN live p ON p.id=d.depends_on_id
             UNION ALL SELECT child.id,0,parent.id,0 FROM live parent JOIN live child ON child.parent_task_id=parent.id
             UNION ALL SELECT parent.id,1,child.id,1 FROM live parent JOIN live child ON child.parent_task_id=parent.id
             UNION ALL SELECT later.id,0,earlier.id,1 FROM ordered later JOIN ordered earlier
               ON later.parent_task_id=earlier.parent_task_id AND later.position=earlier.position+1",
        )
        .bind(project_of)
        .fetch_all(&mut **tx)
        .await?;
        let mut graph = Self {
            index: Default::default(),
            edges: Vec::new(),
        };
        for (from, from_phase, to, to_phase) in rows {
            let from = graph.node(from, from_phase);
            let to = graph.node(to, to_phase);
            graph.edges[from].push(to);
        }
        Ok(graph)
    }

    /// A Task's completion waits on its own start: every completion node
    /// gets that edge with its start node.
    fn node(&mut self, id: String, phase: bool) -> usize {
        if let Some(&node) = self.index.get(&(id.clone(), phase)) {
            return node;
        }
        let node = self.edges.len();
        self.index.insert((id.clone(), phase), node);
        self.edges.push(Vec::new());
        if phase == DONE {
            let start = self.node(id, START);
            self.edges[node].push(start);
        }
        node
    }

    /// Whether `task_id`'s start waits on itself, with `extra` (a candidate
    /// prerequisite's completion) added to its start's waits.
    fn waits_on_itself(&self, task_id: &str, extra: Option<usize>) -> bool {
        let Some(&origin) = self.index.get(&(task_id.to_owned(), START)) else {
            return false;
        };
        let mut seen = vec![false; self.edges.len()];
        let mut stack: Vec<usize> = self.edges[origin].iter().copied().chain(extra).collect();
        while let Some(node) = stack.pop() {
            if node == origin {
                return true;
            }
            if !std::mem::replace(&mut seen[node], true) {
                stack.extend(self.edges[node].iter().copied());
            }
        }
        false
    }
}

/// Whether `task_id` waits on itself, with `candidate` added as one more of
/// its prerequisites (an admission) or as is (a reorder, or a read).
pub(super) async fn dependency_cycle_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    task_id: &str,
    candidate: Option<&str>,
) -> Result<bool> {
    dependency_cycle_any_in_tx(tx, &[task_id], candidate).await
}

/// Whether any of `task_ids` (one Project) waits on itself; `candidate`
/// applies to the first of them.
pub(super) async fn dependency_cycle_any_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    task_ids: &[&str],
    candidate: Option<&str>,
) -> Result<bool> {
    let Some(first) = task_ids.first() else {
        return Ok(false);
    };
    let mut graph = WaitGraph::load(tx, first).await?;
    let extra = match candidate {
        Some(candidate) => {
            let live: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM task WHERE id=? AND deleted_at IS NULL
                   AND json_extract(condition_json,'$.kind') != 'settled')",
            )
            .bind(candidate)
            .fetch_one(&mut **tx)
            .await?;
            live.then(|| graph.node(candidate.to_owned(), DONE))
        }
        None => None,
    };
    Ok(task_ids
        .iter()
        .enumerate()
        .any(|(position, id)| graph.waits_on_itself(id, extra.filter(|_| position == 0))))
}

impl SqliteDb {
    /// Whether `task_id` waits on itself through explicit dependencies,
    /// sibling order and parentage: a wait no completion can end. Admission
    /// refuses such a graph, but a database written before it did, or a
    /// terminal Task that re-opened, can still hold one; the dependency
    /// gate reads this to name the cycle instead of waiting in silence.
    pub async fn dependency_wait_is_cycle(&self, task_id: &str) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let cycle = dependency_cycle_in_tx(&mut tx, task_id, None).await?;
        tx.rollback().await?;
        Ok(cycle)
    }
}

#[async_trait]
impl TaskDependencyRepo for SqliteDb {
    async fn add_dependency(&self, task_id: &str, depends_on_id: &str, now: &str) -> Result<()> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        self.add_dependency_in_tx(&mut transaction, task_id, depends_on_id, now)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn add_dependency_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        task_id: &str,
        depends_on_id: &str,
        now: &str,
    ) -> Result<()> {
        if task_id == depends_on_id {
            return Err(DbError::CycleDetected);
        }

        if dependency_cycle_in_tx(transaction, task_id, Some(depends_on_id)).await? {
            return Err(DbError::CycleDetected);
        }

        sqlx::query(
            "INSERT INTO task_dependency (task_id, depends_on_id, created_at) VALUES (?, ?, ?)",
        )
        .bind(task_id)
        .bind(depends_on_id)
        .bind(now)
        .execute(&mut **transaction)
        .await?;
        Ok(())
    }

    async fn remove_dependency(&self, task_id: &str, depends_on_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM task_dependency WHERE task_id = ? AND depends_on_id = ?")
            .bind(task_id)
            .bind(depends_on_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_dependencies(&self, task_id: &str) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT depends_on_id FROM task_dependency WHERE task_id = ?",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await?)
    }

    async fn list_dependents(&self, depends_on_id: &str) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT task_id FROM task_dependency WHERE depends_on_id = ?",
        )
        .bind(depends_on_id)
        .fetch_all(&self.pool)
        .await?)
    }

    async fn list_dependency_tasks(&self, task_id: &str) -> Result<Vec<Task>> {
        let columns = task_columns_with_alias("t");
        let sql = format!(
            "SELECT {columns}
             FROM task_dependency AS td
             JOIN task AS t ON t.id = td.depends_on_id
             WHERE td.task_id = ? AND t.deleted_at IS NULL
             ORDER BY td.created_at ASC, t.id ASC"
        );
        let rows = sqlx::query(&sql)
            .bind(task_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(map_task).collect()
    }

    async fn list_dependent_tasks(&self, depends_on_id: &str) -> Result<Vec<Task>> {
        let columns = task_columns_with_alias("t");
        let sql = format!(
            "SELECT {columns}
             FROM task_dependency AS td
             JOIN task AS t ON t.id = td.task_id
             WHERE td.depends_on_id = ? AND t.deleted_at IS NULL
             ORDER BY td.created_at ASC, t.id ASC"
        );
        let rows = sqlx::query(&sql)
            .bind(depends_on_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(map_task).collect()
    }

    async fn unsatisfied_dependencies(&self, task_id: &str) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT depends_on_id FROM task_dependency WHERE task_id = ? AND depends_on_id NOT IN (SELECT id FROM task WHERE status = 'done')",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await?)
    }
}

fn task_columns_with_alias(alias: &str) -> String {
    TASK_COLUMNS
        .split(", ")
        .map(|column| format!("{alias}.{column}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct NativeTaskDependencyRow {
    row: sqlx::sqlite::SqliteRow,
}
impl NativeTaskDependencyRow {
    pub fn task_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("task_id")?)
    }
    pub fn depends_on_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("depends_on_id")?)
    }
}

impl SqliteDb {
    pub async fn native_work_task_dependencies(
        &self,
        ids: &[String],
    ) -> crate::Result<Vec<NativeTaskDependencyRow>> {
        let placeholders = vec!["?"; ids.len()].join(", ");
        let sql = format!(
            "SELECT task_id, depends_on_id FROM task_dependency
                 WHERE task_id IN ({placeholders}) ORDER BY task_id, depends_on_id"
        );
        let mut query = sqlx::query(&sql);
        for id in ids {
            query = query.bind(id);
        }

        let rows = query.fetch_all(self.pool()).await?;
        Ok(rows
            .into_iter()
            .map(|row| NativeTaskDependencyRow { row })
            .collect())
    }
}
