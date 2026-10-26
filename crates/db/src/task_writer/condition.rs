//! SQL adapters carry required reload intent independently of an optional statement.
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
        let mut query = Self::new(db, task_id, query, change);
        query.condition = Some(condition);
        query
    }
    pub(super) fn into_mutation(self) -> (SqliteDb, String, TaskMutation) {
        let mutation = mutation(
            &self.task_id,
            self.query,
            self.arguments,
            self.change,
            self.condition,
        );
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
        let mut query = Self::new(db, query, change);
        query.condition = Some(condition);
        query
    }
}

pub(super) fn mutation(
    task_id: &str,
    query: String,
    arguments: Vec<Value>,
    change: ConditionChange,
    condition: Option<ConditionStatement>,
) -> TaskMutation {
    match condition {
        Some(condition) => TaskMutation::StatedSql {
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
            change,
        },
    }
}

pub(super) async fn produce_in_tx(
    db: &SqliteDb,
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &str,
    change: ConditionChange,
    condition: Option<&ConditionStatement>,
) -> Result<()> {
    if let Some(statement) = condition {
        db.state_task_condition_in_tx(tx, task_id, change, statement)
            .await
    } else {
        crate::task_condition::produce(tx, task_id, change).await
    }
}
