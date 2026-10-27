//! Explicit condition envelopes survive durable mutation serialization. The
//! scoped statement is consumed by the existing in-transaction producer seam.
use super::*;
use crate::{Conditioned, TaskMutation};

#[derive(Clone)]
struct AppliedCondition {
    source_version: i64,
    source_digest: String,
    condition: TaskCondition,
}

#[derive(Clone)]
struct StatementScope {
    task_id: String,
    statement: ConditionStatement,
    expected_version: Option<i64>,
    applied: std::cell::RefCell<Option<AppliedCondition>>,
    version_checked: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
tokio::task_local! { static STATEMENT: StatementScope; }

pub(super) fn current_statement(task_id: &str) -> Option<ConditionStatement> {
    STATEMENT
        .try_with(|scope| {
            (scope.task_id == task_id).then(|| {
                if scope.applied.borrow().is_some() {
                    ConditionStatement::Preserve
                } else {
                    scope.statement.clone()
                }
            })
        })
        .ok()
        .flatten()
}

pub(super) fn applied_condition(
    task_id: &str,
    stored: &[u8],
    version: i64,
) -> Option<TaskCondition> {
    STATEMENT
        .try_with(|scope| {
            if scope.task_id != task_id {
                return None;
            }
            scope
                .applied
                .borrow()
                .as_ref()
                .filter(|applied| {
                    applied.source_version == version
                        && applied.source_digest == diagnostics::digest(stored)
                })
                .map(|applied| applied.condition.clone())
        })
        .ok()
        .flatten()
}

pub(super) fn mark_statement_applied(
    task_id: &str,
    condition: &TaskCondition,
    stored: &[u8],
    version: i64,
) {
    let _ = STATEMENT.try_with(|scope| {
        if scope.task_id == task_id {
            *scope.applied.borrow_mut() = Some(AppliedCondition {
                source_version: version,
                source_digest: diagnostics::digest(stored),
                condition: condition.clone(),
            });
        }
    });
}

pub(crate) async fn with_statement<T>(
    task_id: &str,
    statement: ConditionStatement,
    future: impl std::future::Future<Output = T>,
) -> T {
    with_statement_at_version(task_id, statement, None, future).await
}

pub(crate) async fn with_statement_at_version<T>(
    task_id: &str,
    statement: ConditionStatement,
    expected_version: Option<i64>,
    future: impl std::future::Future<Output = T>,
) -> T {
    STATEMENT
        .scope(
            StatementScope {
                task_id: task_id.to_owned(),
                statement,
                expected_version,
                applied: std::cell::RefCell::new(None),
                version_checked: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            },
            future,
        )
        .await
}

pub(crate) async fn fence_statement_version_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    task_id: &str,
) -> Result<()> {
    use std::sync::atomic::Ordering;
    let fence = STATEMENT
        .try_with(|scope| {
            (scope.task_id == task_id && !scope.version_checked.load(Ordering::SeqCst))
                .then(|| {
                    scope
                        .expected_version
                        .map(|v| (v, scope.version_checked.clone()))
                })
                .flatten()
        })
        .ok()
        .flatten();
    if let Some((expected, checked)) = fence {
        let actual: i64 = sqlx::query_scalar("SELECT version FROM task WHERE id=?")
            .bind(task_id)
            .fetch_optional(&mut **tx)
            .await?
            .ok_or(DbError::NotFound)?;
        if actual != expected {
            return Err(DbError::VersionConflict);
        }
        checked.store(true, Ordering::SeqCst);
    }
    Ok(())
}

impl SqliteDb {
    /// The statement and mutation share the existing writer's transaction and
    /// fences. When queued, both are durable in the same TaskMutation payload.
    pub async fn apply_conditioned_task_mutation<T: serde::de::DeserializeOwned>(
        &self,
        task_id: &str,
        input: Conditioned<TaskMutation>,
    ) -> Result<T> {
        let mutation = TaskMutation::Conditioned {
            task_id: task_id.to_owned(),
            condition: input.condition,
            mutation: Box::new(input.input),
        };
        if crate::task_writer::owns_task(task_id) {
            serde_json::from_value(mutation.apply_unrebased(self).await?)
                .map_err(|e| DbError::Check(e.to_string()))
        } else {
            self.run_task_mutation(task_id, mutation).await
        }
    }

    /// Typed statement for a direct writer that has already retained its own
    /// CAS/epoch/assignment/step fences in this transaction.
    pub async fn state_task_condition_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        task_id: &str,
        change: ConditionChange,
        statement: &ConditionStatement,
    ) -> Result<()> {
        if statement.owner_stated() {
            return Err(DbError::Check(
                "integration/check require their claimed-step seam".into(),
            ));
        }
        self.fence_task_lease_in_tx(tx, task_id, "typed condition statement")
            .await?;
        let produced = producers::derive(tx, task_id, change, None, Some(statement))
            .await?
            .ok_or(DbError::NotFound)?;
        set_condition(
            tx,
            task_id,
            &produced.encoded,
            &produced.input.channel_digest(),
            Some(produced.version),
        )
        .await?;
        if let Some(parent) = produced.parent {
            produce_children(tx, &parent).await?;
        }
        Ok(())
    }
}

pub(super) async fn record_statement_text(
    c: &mut SqliteConnection,
    task_id: &str,
    statement: &ConditionStatement,
) -> Result<()> {
    let mut texts: Vec<(&str, &str, DiagnosticOwner)> = Vec::new();
    if let ConditionStatement::Hold { actor, reason, .. } = statement {
        texts.push((
            "hold",
            reason,
            DiagnosticOwner::Reason {
                reason: ParkReason::Held {
                    actor: actor.clone(),
                },
            },
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for (source, text, owner) in texts {
        if !seen.insert(text) {
            continue;
        }
        let kind = format!("{source}_reason");
        diagnostics::record(
            c,
            task_id,
            RecordTaskDiagnosticText {
                kind: &kind,
                source,
                owner: &owner,
                text,
            },
        )
        .await?;
    }
    Ok(())
}
