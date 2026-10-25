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
    /// Identity read under the caller's Task CAS, without any legacy input.
    pub async fn task_condition_entry_identity(
        &self,
        task_id: &str,
        expected_version: i64,
    ) -> Result<ConditionIdentity> {
        let epoch: i64 =
            sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=? AND version=?")
                .bind(task_id)
                .bind(expected_version)
                .fetch_optional(self.pool())
                .await?
                .ok_or(DbError::VersionConflict)?;
        Ok(ConditionIdentity::Entry {
            epoch,
            transition_id: None,
        })
    }

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
        set_condition(tx, task_id, &produced.encoded, Some(produced.version)).await?;
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
    match statement {
        ConditionStatement::Hold { actor, reason, .. } => texts.push((
            "hold",
            reason,
            DiagnosticOwner::Reason {
                reason: ParkReason::Held {
                    actor: actor.clone(),
                },
            },
        )),
        ConditionStatement::ReplaceDiagnostic { replacement, .. } => {
            let edit = ConditionStatement::Edit {
                edit: Box::new(ConditionOwnerEdit {
                    owner: ConditionReasonOwner::Diagnostic,
                    expected: None,
                    replacement: Some(*replacement.clone()),
                }),
            };
            return Box::pin(record_statement_text(c, task_id, &edit)).await;
        }
        ConditionStatement::Edit { edit } => {
            if let Some(replacement) = &edit.replacement {
                let presentation = match &replacement.condition {
                    StatedCondition::Parked { presentation, .. }
                    | StatedCondition::Failed { presentation, .. }
                    | StatedCondition::Deferred { presentation, .. } => presentation,
                };
                let reason = match &replacement.condition {
                    StatedCondition::Parked { reason, .. }
                    | StatedCondition::Failed { reason, .. } => Some(reason.clone()),
                    _ => None,
                };
                let owner = reason
                    .map(|reason| DiagnosticOwner::Reason { reason })
                    .unwrap_or(DiagnosticOwner::StoredCondition);
                let source = match edit.owner {
                    ConditionReasonOwner::Hold => "hold",
                    ConditionReasonOwner::Diagnostic => "diagnostic",
                    ConditionReasonOwner::Budget { .. } => "budget",
                    ConditionReasonOwner::Entry => "entry",
                    ConditionReasonOwner::Human => "human",
                    ConditionReasonOwner::Retry => "retry",
                    ConditionReasonOwner::Dispatch => "dispatch",
                    ConditionReasonOwner::Placement => "placement",
                    ConditionReasonOwner::Environment => "environment",
                    ConditionReasonOwner::OwnerConnection => "owner_connection",
                    ConditionReasonOwner::PlanTransport => "plan_transport",
                    ConditionReasonOwner::Dependencies => "dependencies",
                    ConditionReasonOwner::Coordination => "coordination",
                    ConditionReasonOwner::ProjectPause => "project_pause",
                };
                if let Some(d) = &presentation.diagnostic {
                    texts.push((source, &d.blocking_reason, owner.clone()));
                    if let Some(message) = &d.message {
                        texts.push((source, message, owner.clone()));
                    }
                }
                if let Some(i) = &presentation.interruption {
                    texts.push((source, &i.reason, owner.clone()));
                }
                if let Some(retry) = &presentation.retry {
                    texts.push((source, &retry.reason, owner.clone()));
                }
                if let Some(refusal) = &presentation.refusal {
                    texts.push((source, &refusal.safe_message, owner.clone()));
                }
                if let Some(failure) = &presentation.failure_message {
                    texts.push((source, failure, owner));
                }
            }
        }
        _ => {}
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
