//! Required-intent constructors for converted SQL aggregates. Historical Sql
//! payloads retain their deployed decoder and replay path until 5b translates them.
use super::*;
use crate::{ConditionChange, ConditionStatement};

impl TaskQuery {
    pub fn stated(
        db: &SqliteDb,
        task_id: &str,
        query: impl Into<String>,
        change: ConditionChange,
        condition: ConditionStatement,
    ) -> Self {
        let mut query = Self::new(db, task_id, query);
        query.condition = Some((change, condition));
        query
    }
    pub(super) fn into_mutation(self) -> (SqliteDb, String, TaskMutation) {
        let mutation = mutation(&self.task_id, self.query, self.arguments, self.condition);
        (self.db, self.task_id, mutation)
    }
}
impl BulkTaskQuery {
    pub fn stated(
        db: &SqliteDb,
        query: impl Into<String>,
        change: ConditionChange,
        condition: ConditionStatement,
    ) -> Self {
        let mut query = Self::new(db, query);
        query.condition = Some((change, condition));
        query
    }
}

pub(super) fn mutation(
    task_id: &str,
    query: String,
    arguments: Vec<Value>,
    condition: Option<(ConditionChange, ConditionStatement)>,
) -> TaskMutation {
    match condition {
        Some((change, condition)) => TaskMutation::StatedSql {
            task_id: task_id.to_owned(),
            query,
            arguments,
            change,
            condition,
        },
        None => TaskMutation::Sql {
            task_id: task_id.to_owned(),
            query,
            arguments,
        },
    }
}

pub(super) async fn produce_in_tx(
    db: &SqliteDb,
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &str,
    query: &str,
    condition: Option<&(ConditionChange, ConditionStatement)>,
) -> Result<()> {
    if let Some((change, statement)) = condition {
        db.state_task_condition_in_tx(tx, task_id, *change, statement)
            .await
    } else if let Some(change) = crate::task_condition::sql_change(query) {
        crate::task_condition::produce(tx, task_id, change).await
    } else {
        Ok(())
    }
}
