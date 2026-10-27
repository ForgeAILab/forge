//! Ledger budget accounting operations.
use super::*;

pub async fn spent(pool: &sqlx::SqlitePool, task: &str, key: &str) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT spent FROM task_budget WHERE task_id = ? AND kind = ?")
            .bind(task)
            .bind(key)
            .fetch_optional(pool)
            .await?
            .unwrap_or(0),
    )
}
/// Whether `step` already holds a receipt for `key` in the Task's current
/// budget window: the charge it names was made and is not made again.
pub async fn receipt_recorded(
    pool: &sqlx::SqlitePool,
    task: &str,
    key: &str,
    step: &str,
) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM task_budget_charge c JOIN task_budget b USING(task_id,kind) WHERE c.task_id=? AND c.kind=? AND c.window_id=b.window_id AND c.step_id=?)",
    )
    .bind(task)
    .bind(key)
    .bind(step)
    .fetch_one(pool)
    .await?)
}
pub async fn load(pool: &sqlx::SqlitePool, task: &str) -> Result<HashMap<String, i64>> {
    let rows = sqlx::query("SELECT kind, spent FROM task_budget WHERE task_id = ?")
        .bind(task)
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|r| Ok((r.try_get("kind")?, r.try_get("spent")?)))
        .collect()
}
/// Spending for a page of Tasks in one query, keyed by Task id.
pub async fn load_many(
    pool: &sqlx::SqlitePool,
    tasks: &[&str],
) -> Result<HashMap<String, HashMap<String, i64>>> {
    let ids = serde_json::to_string(tasks).map_err(|e| DbError::Check(e.to_string()))?;
    let rows = sqlx::query(
        "SELECT task_id, kind, spent FROM task_budget WHERE task_id IN (SELECT value FROM json_each(?))",
    )
    .bind(ids)
    .fetch_all(pool)
    .await?;
    let mut spent = HashMap::<String, HashMap<String, i64>>::new();
    for row in rows {
        spent
            .entry(row.try_get("task_id")?)
            .or_default()
            .insert(row.try_get("kind")?, row.try_get("spent")?);
    }
    Ok(spent)
}
pub async fn reset(
    tx: &mut Transaction<'_, Sqlite>,
    task: &str,
    key: &str,
    window: &str,
) -> Result<()> {
    sqlx::query("INSERT INTO task_budget(task_id,kind,window_id,spent) VALUES(?,?,?,0) ON CONFLICT(task_id,kind) DO UPDATE SET window_id=excluded.window_id, spent=0 WHERE task_budget.window_id != excluded.window_id")
        .bind(task).bind(key).bind(window).execute(&mut **tx).await?;
    crate::task_condition::produce(tx, task, crate::ConditionChange::Budget).await?;
    Ok(())
}
pub async fn reset_all(tx: &mut Transaction<'_, Sqlite>, task: &str, window: &str) -> Result<()> {
    for kind in Kind::PERSISTED {
        reset(tx, task, kind.key(), window).await?;
    }
    sqlx::query("UPDATE task_budget SET window_id=?,spent=0 WHERE task_id=? AND window_id!=?")
        .bind(window)
        .bind(task)
        .bind(window)
        .execute(&mut **tx)
        .await?;
    crate::task_condition::produce(tx, task, crate::ConditionChange::Budget).await?;
    Ok(())
}
/// The receipt identity a failed review entry charges under. The cascade that
/// bounces that entry back reuses it, so one failed entry is charged once.
pub fn entry_charge_step(hook_step: &str) -> String {
    format!("entry:{hook_step}")
}
/// A fresh state entry (any status write that does not carry the previous entry
/// barrier) starts a new review-CI interruption episode. An entry retry keeps
/// its barrier and with it the episode's spending.
pub async fn fresh_entry(tx: &mut Transaction<'_, Sqlite>, task: &str, window: &str) -> Result<()> {
    reset(tx, task, Kind::ReviewCiInfrastructure.key(), window).await
}
/// Record an outcome the workflow already admitted (a logged rejection, an
/// exhausting verdict, a queued pre-upgrade increment): it always spends once
/// per receipt, whatever the limit.
pub async fn admit(
    tx: &mut Transaction<'_, Sqlite>,
    task: &str,
    key: &str,
    step: &str,
) -> Result<bool> {
    Ok(charge(tx, task, key, i64::MAX, step).await?.charged)
}
/// This is the only persisted charge point. Its receipt and consuming write share
/// the caller's authority-fenced transaction. Step identities include effect ordinals.
/// A charge that would exceed `limit` is refused (not recorded).
pub async fn charge(
    tx: &mut Transaction<'_, Sqlite>,
    task: &str,
    key: &str,
    limit: i64,
    step: &str,
) -> Result<Charge> {
    if [Kind::ReportCorrection, Kind::ReviewCheckRerun]
        .iter()
        .any(|kind| kind.key() == key)
    {
        return Err(DbError::Check(format!("{key} is invocation-scoped")));
    }
    sqlx::query("INSERT INTO task_budget(task_id,kind,window_id,spent) VALUES(?,?,COALESCE((SELECT window_id FROM task_budget WHERE task_id=? AND kind='execution'),'initial'),0) ON CONFLICT DO NOTHING")
        .bind(task).bind(key).bind(task).execute(&mut **tx).await?;
    let (window, old): (String, i64) =
        sqlx::query_as("SELECT window_id,spent FROM task_budget WHERE task_id=? AND kind=?")
            .bind(task)
            .bind(key)
            .fetch_one(&mut **tx)
            .await?;
    let recorded: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_budget_charge WHERE task_id=? AND kind=? AND window_id=? AND step_id=?)")
        .bind(task).bind(key).bind(&window).bind(step).fetch_one(&mut **tx).await?;
    if recorded || !allows_retry(limit, old) {
        return Ok(Charge {
            spent: old,
            remaining: remaining(limit, old),
            charged: false,
        });
    }
    sqlx::query("INSERT INTO task_budget_charge(task_id,kind,window_id,step_id) VALUES(?,?,?,?)")
        .bind(task)
        .bind(key)
        .bind(&window)
        .bind(step)
        .execute(&mut **tx)
        .await?;
    let spent = old.saturating_add(1);
    sqlx::query("UPDATE task_budget SET spent=? WHERE task_id=? AND kind=?")
        .bind(spent)
        .bind(task)
        .bind(key)
        .execute(&mut **tx)
        .await?;
    // The charge is authoritative; the shadow never refuses it.
    crate::task_condition::produce_best_effort(tx, task, crate::ConditionChange::Budget).await;
    Ok(Charge {
        spent,
        remaining: remaining(limit, spent),
        charged: true,
    })
}
pub async fn apply(tx: &mut Transaction<'_, Sqlite>, task: &str, effect: Mutation) -> Result<bool> {
    match effect {
        Mutation::Reset { key, window } => {
            let old: Option<(String, i64)> = sqlx::query_as(
                "SELECT window_id,spent FROM task_budget WHERE task_id=? AND kind=?",
            )
            .bind(task)
            .bind(&key)
            .fetch_optional(&mut **tx)
            .await?;
            reset(tx, task, &key, &window).await?;
            Ok(old.is_some_and(|(previous, spent)| previous != window && spent != 0))
        }
        Mutation::Charge { key, limit, step } => {
            let result = charge(tx, task, &key, limit, &step).await?;
            if !result.charged && !allows_retry(limit, result.spent) {
                let exists: bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_budget_charge c JOIN task_budget b USING(task_id,kind) WHERE c.task_id=? AND c.kind=? AND c.window_id=b.window_id AND c.step_id=?)")
                    .bind(task).bind(&key).bind(&step).fetch_one(&mut **tx).await?;
                if !exists {
                    return Err(DbError::Check(format!("{key} retry budget exhausted")));
                }
            }
            Ok(result.charged)
        }
    }
}

/// The invocation starts with its first request, including a verify that passes
/// without a rerun. Replaying the same Review never refunds an existing charge.
pub async fn initialize_verify_budget(
    db: &SqliteDb,
    task: &Task,
    workflow: &WorkflowDefinition,
    review_id: &str,
) -> Result<()> {
    let mut tx = verify_budget_transaction(db, task, workflow, review_id).await?;
    reset(&mut tx, &task.id, Kind::ReviewVerifyRerun.key(), review_id).await?;
    tx.commit().await?;
    Ok(())
}

async fn verify_budget_transaction(
    db: &SqliteDb,
    task: &Task,
    workflow: &WorkflowDefinition,
    review_id: &str,
) -> Result<Transaction<'static, Sqlite>> {
    if !workflow
        .states
        .iter()
        .any(|state| state.name == task.status && is_review_state(state))
    {
        return Err(DbError::InvalidTransition);
    }
    let mut tx = crate::begin_immediate(db.pool()).await?;
    if let Some(step) = crate::task_writer::current_task_step().filter(|s| s.task_id == task.id) {
        db.fence_hook_in_tx(&mut tx, &step).await?;
    }
    let current: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task t JOIN review r ON r.task_id=t.id WHERE t.id=? AND t.version=? AND t.status=? AND t.deleted_at IS NULL AND r.id=? AND r.status='running' AND NOT EXISTS(SELECT 1 FROM review newer WHERE newer.task_id=t.id AND newer.attempt_number>r.attempt_number))")
        .bind(&task.id).bind(task.version).bind(&task.status).bind(review_id).fetch_one(&mut *tx).await?;
    if !current {
        return Err(DbError::VersionConflict);
    }
    Ok(tx)
}

/// Admit one further verify evaluation. The Review id is the invocation window
/// and the evaluated result is the receipt: replay after a checkpoint gap must
/// return the same admission without charging twice.
pub async fn grant_verify_rerun(
    db: &SqliteDb,
    task: &Task,
    workflow: &WorkflowDefinition,
    state_config: &Value,
    review_id: &str,
    evaluation: u32,
) -> Result<bool> {
    let mut tx = verify_budget_transaction(db, task, workflow, review_id).await?;
    let key = Kind::ReviewVerifyRerun.key();
    let receipt = format!("verify:{review_id}:{evaluation}");
    reset(&mut tx, &task.id, key, review_id).await?;
    let paid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_budget_charge WHERE task_id=? AND kind=? AND window_id=? AND step_id=?)")
        .bind(&task.id).bind(key).bind(review_id).bind(&receipt).fetch_one(&mut *tx).await?;
    let admitted = paid
        || charge(
            &mut tx,
            &task.id,
            key,
            i64::from(limit(
                task,
                Kind::ReviewVerifyRerun,
                Some(state_config),
                None,
            )?),
            &receipt,
        )
        .await?
        .charged;
    tx.commit().await?;
    Ok(admitted)
}
/// Upgrade pending, typed DB effects atomically with their retained ledger.
/// Arbitrary metadata Set values and opaque command arguments are never traversed.
pub(crate) async fn migrate_pending_mutations(tx: &mut Transaction<'_, Sqlite>) -> Result<()> {
    let rows:Vec<(String,String,String)>=sqlx::query_as("SELECT id,task_id,payload_json FROM task_step WHERE kind='mutation' AND status IN ('pending','claimed','failed')").fetch_all(&mut **tx).await?;
    for (step, task_id, raw) in rows {
        let mut payload: Value =
            serde_json::from_str(&raw).map_err(|e| DbError::Check(e.to_string()))?;
        let Some(root) = payload.as_object_mut() else {
            continue;
        };
        let Some((name, args)) = root.iter_mut().next() else {
            continue;
        };
        if !name.starts_with("TaskMutateMetadata")
            && !name.starts_with("TaskUpdateRecoveryMetadata")
        {
            continue;
        }
        let Some(args) = args.as_object_mut() else {
            continue;
        };
        let field = if args.contains_key("mutations") {
            "mutations"
        } else {
            "metadata_mutations"
        };
        let Some(mutations) = args.get_mut(field) else {
            continue;
        };
        let original: Vec<crate::TaskMetadataMutation> =
            serde_json::from_value(mutations.clone()).map_err(|e| DbError::Check(e.to_string()))?;
        let mut changed = false;
        let mut converted = Vec::new();
        for (ordinal, mutation) in original.into_iter().enumerate() {
            let replacement = match mutation {
                crate::TaskMetadataMutation::Increment { key, by: 1 }
                    if key == "execution_retry_count" || key == "workflow_guard_retry_count" =>
                {
                    changed = true;
                    let kind = if key == "execution_retry_count" {
                        Kind::Execution
                    } else {
                        Kind::WorkflowGuard
                    };
                    // The old binary admitted this increment before queuing
                    // it and applied it unconditionally: it is not re-checked.
                    crate::TaskMetadataMutation::Budget(Mutation::Charge {
                        key: kind.key().into(),
                        limit: i64::MAX,
                        step: format!("upgrade:{step}:{ordinal}"),
                    })
                }
                crate::TaskMetadataMutation::CompareAndMutate {
                    key,
                    expected,
                    mut mutations,
                } if key == "execution_retry_count" || key == "workflow_guard_retry_count" => {
                    changed = true;
                    let kind = if key == "execution_retry_count" {
                        Kind::Execution
                    } else {
                        Kind::WorkflowGuard
                    };
                    let window: String = sqlx::query_scalar(
                        "SELECT window_id FROM task_budget WHERE task_id=? AND kind=?",
                    )
                    .bind(&task_id)
                    .bind(kind.key())
                    .fetch_one(&mut **tx)
                    .await?;
                    if kind == Kind::WorkflowGuard {
                        mutations.push(crate::TaskMetadataMutation::Budget(Mutation::Reset {
                            key: kind.key().into(),
                            window: format!("success:upgrade:{step}"),
                        }));
                    }
                    crate::TaskMetadataMutation::BudgetIfSpent {
                        key: kind.key().into(),
                        expected: expected
                            .as_u64()
                            .map(|n| n.min(i64::MAX as u64) as i64)
                            .unwrap_or(0),
                        window_id: window,
                        mutations,
                    }
                }
                mutation => mutation,
            };
            converted.push(replacement);
        }
        if changed {
            *mutations =
                serde_json::to_value(converted).map_err(|e| DbError::Check(e.to_string()))?;
            sqlx::query("UPDATE task_step SET payload_json=? WHERE id=?")
                .bind(payload.to_string())
                .bind(step)
                .execute(&mut **tx)
                .await?;
        }
    }
    Ok(())
}
