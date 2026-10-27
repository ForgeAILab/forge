//! Review budget accounting operations.
use super::*;

/// Preserve the reviewer finding policy at the charge point. A verdict was
/// already admitted, so it always spends; whether it exhausts the budget, and
/// so opens a recovery episode, is decided where the limit is resolved
/// (`review_failure_target`).
pub async fn review_verdict(
    tx: &mut Transaction<'_, Sqlite>,
    task: &Task,
    review: &Review,
    status: &ReviewStatus,
    details: &Value,
    owner: bool,
) -> Result<()> {
    if *status == ReviewStatus::Passed {
        if let Some(contract) = details
            .pointer("/conformance/contract/execution_id")
            .and_then(Value::as_str)
        {
            reset(tx, &task.id, Kind::ReviewCarry.key(), contract).await?;
        }
        return Ok(());
    }
    if *status != ReviewStatus::Failed
        || owner
        || owner_step(&task.id)
        || details.pointer("/verify/status").and_then(Value::as_str) == Some("inconclusive")
    {
        return Ok(());
    }
    let latest: Option<String> = sqlx::query_scalar(
        "SELECT id FROM review WHERE task_id=? ORDER BY attempt_number DESC,id DESC LIMIT 1",
    )
    .bind(&task.id)
    .fetch_optional(&mut **tx)
    .await?;
    if latest.as_deref() != Some(review.id.as_str()) || details.get("execution_failure").is_some() {
        return Ok(());
    }
    if owner_requested_review_entry(tx, &task.id, None, Some(&review.created_at)).await? {
        return Ok(());
    }
    if let Some(c) = details.get("conformance").filter(|v| !v.is_null()) {
        let conformance: api_types::ReviewConformance =
            serde_json::from_value(c.clone()).map_err(|e| DbError::Check(e.to_string()))?;
        if conformance.status == api_types::ConformanceStatus::Blocked {
            return Ok(());
        }
        let window_reviews = SqliteDb::review_window_in_tx(tx, &task.id).await?.reviews;
        let previous_reviews = sqlx::query("SELECT * FROM review WHERE task_id=? AND attempt_number<? ORDER BY attempt_number DESC,id DESC LIMIT 1")
            .bind(&task.id).bind(review.attempt_number).fetch_all(&mut **tx).await?
            .into_iter().map(crate::sqlite::map_review).collect::<Result<Vec<_>>>()?;
        if owner_review_failure_message(
            &conformance,
            review,
            &previous_reviews,
            &window_reviews,
            !coordination_root_in_tx(tx, task).await?,
        )
        .is_some()
        {
            return Ok(());
        }
    }
    admit(
        tx,
        &task.id,
        Kind::Review.key(),
        &format!("review:{}", review.id),
    )
    .await?;
    Ok(())
}

/// The review budget ran out on `review`: open that exhaustion's automatic
/// recovery episode. Idempotent per exhausting verdict.
pub async fn open_recovery_episode(db: &SqliteDb, task: &str, review: &str) -> Result<()> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    db.fence_task_lease_in_tx(&mut tx, task, "review recovery episode")
        .await?;
    open_recovery_episode_in_tx(&mut tx, task, review).await?;
    tx.commit().await?;
    Ok(())
}
pub(super) async fn open_recovery_episode_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    task: &str,
    review: &str,
) -> Result<()> {
    reset(
        tx,
        task,
        Kind::AutomaticReviewRecovery.key(),
        &format!("exhaustion:{review}"),
    )
    .await
}

/// Charge the budget a committed transition consumed. Every charge here is an
/// outcome the workflow already admitted, so limits are not consulted.
///
/// `owner` is the transition actor's typed owner authority
/// ([`api_types::Actor::is_owner`]): an owner's move spends nothing.
/// `entry_step` is the hook step whose failed review entry produced this
/// cascade; the bounce shares that entry's receipt instead of charging twice.
#[allow(clippy::too_many_arguments)]
pub async fn transition(
    tx: &mut Transaction<'_, Sqlite>,
    task: &str,
    from: &str,
    owner: bool,
    bridge: &api_types::TransitionBridge,
    rejection: bool,
    step: &str,
    entry_step: Option<&str>,
) -> Result<()> {
    if bridge.bridge_kind == Some(api_types::TransitionBridgeKind::RetryWindowReset) {
        return reset_all(tx, task, step).await;
    }
    if owner {
        return Ok(());
    }
    let from_review = if rejection {
        let raw: String = sqlx::query_scalar(
            "SELECT p.workflow_definition FROM project p JOIN task t ON t.project_id=p.id WHERE t.id=?",
        ).bind(task).fetch_one(&mut **tx).await?;
        review_state_from_json(&raw, from)
    } else {
        false
    };
    if from_review {
        if owner_requested_review_entry(tx, task, Some(step), None).await? {
            return Ok(());
        }
        let latest:Option<(String,String)>=sqlx::query_as("SELECT id,status FROM review WHERE task_id=? ORDER BY attempt_number DESC,id DESC LIMIT 1").bind(task).fetch_optional(&mut **tx).await?;
        // Failed verdict settlement is the charge point for failures. This branch
        // retains only the already-existing gate-outcome debit (or a gate with no Review).
        if latest.as_ref().is_none_or(|(_, status)| status != "failed") {
            let receipt = entry_step.map(entry_charge_step);
            admit(
                tx,
                task,
                Kind::Review.key(),
                receipt.as_deref().unwrap_or(step),
            )
            .await?;
        }
        admit(tx, task, REVIEW_GATE_KEY, step).await?;
        return Ok(());
    }
    let key = match bridge.bridge_kind {
        Some(api_types::TransitionBridgeKind::TargetMovedRebase) => {
            Some(Kind::TargetMovedRebase.key().to_owned())
        }
        Some(api_types::TransitionBridgeKind::ConflictHandoff) => {
            Some(Kind::ConflictHandoff.key().to_owned())
        }
        _ if rejection && from == "merging" && !bridge.is_review_refresh() => {
            admit(tx, task, &gate_key(from), step).await?;
            Some(Kind::MergeFix.key().to_owned())
        }
        _ if rejection => Some(gate_key(from)),
        _ => None,
    };
    if let Some(key) = key {
        admit(tx, task, &key, step).await?;
    }
    Ok(())
}
/// Whether the owner asked for the review entry the Task is in, or the one it
/// is leaving: a review re-run records who requested it on the entry's
/// transition row. Such an entry is routed as the workflow's own, but nothing
/// it causes (red checks, a failing reviewer, the bounce to remediation)
/// spends review retry budget. `leaving` is the transition row being written
/// by the move out of `review`; `since` limits the answer to a Review the
/// entry itself opened.
pub async fn owner_requested_review_entry(
    tx: &mut Transaction<'_, Sqlite>,
    task: &str,
    leaving: Option<&str>,
    since: Option<&str>,
) -> Result<bool> {
    Ok(review_rerun_entry(&mut **tx, task, leaving)
        .await?
        .is_some_and(|(requested_by, entered_at)| {
            requested_by.is_owner() && since.is_none_or(|opened| opened >= entered_at.as_str())
        }))
}

/// The latest entry of the Task into `review`, when it is a review re-run:
/// who asked for it and when it began. `leaving` excludes the transition row
/// a move out of `review` is writing. A Review created before that time is
/// not this entry's.
pub async fn review_rerun_entry<'e, E>(
    executor: E,
    task: &str,
    leaving: Option<&str>,
) -> Result<Option<(api_types::Actor, String)>>
where
    E: sqlx::Executor<'e, Database = Sqlite>,
{
    // Retain only the latest entry per state, then resolve the review state
    // from the Project workflow. Recovery markers have no entry epoch.
    let entries = sqlx::query(
        "WITH entries AS (
            SELECT id,bridge_kind,bridge_payload,created_at,to_state,status_epoch,
                   ROW_NUMBER() OVER (PARTITION BY to_state ORDER BY status_epoch DESC,created_at DESC,rowid DESC) AS ordinal
            FROM transition_log WHERE task_id=? AND status_epoch IS NOT NULL AND id IS NOT ?
         )
         SELECT e.*,p.workflow_definition FROM entries e
         JOIN task t ON t.id=? JOIN project p ON p.id=t.project_id
         WHERE e.ordinal=1 ORDER BY e.status_epoch DESC,e.created_at DESC",
    )
    .bind(task)
    .bind(leaving)
    .bind(task)
    .fetch_all(executor)
    .await?;
    for entry in entries {
        let workflow: String = entry.try_get("workflow_definition")?;
        let state: String = entry.try_get("to_state")?;
        if !review_state_from_json(&workflow, &state) {
            continue;
        }
        let id: String = entry.try_get("id")?;
        let kind: Option<String> = entry.try_get("bridge_kind")?;
        let payload: Option<String> = entry.try_get("bridge_payload")?;
        let entered_at: String = entry.try_get("created_at")?;
        return Ok(
            crate::decode_transition_bridge(&id, kind.as_deref(), payload.as_deref())
                .ok()
                .and_then(|bridge| bridge.review_rerun_requester())
                .map(|requested_by| (requested_by, entered_at)),
        );
    }
    Ok(None)
}
// Charging uses exactly the planning-role resolution used by services.
pub(super) async fn coordination_root_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    task: &Task,
) -> Result<bool> {
    if task.parent_task_id.is_some() {
        return Ok(false);
    }
    let raw: String = sqlx::query_scalar("SELECT workflow_definition FROM project WHERE id=?")
        .bind(&task.project_id)
        .fetch_one(&mut **tx)
        .await?;
    let Some(role) = WorkflowDefinition::coordination_planning_role_from_json(&raw) else {
        return Ok(false);
    };
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_role_assignment a
        WHERE a.task_id=? AND a.role_name=? AND a.assignee_type='agent' AND a.assignee_id IS NOT NULL
        AND EXISTS(SELECT 1 FROM task child WHERE child.parent_task_id=a.task_id AND child.deleted_at IS NULL))")
        .bind(&task.id).bind(role).fetch_one(&mut **tx).await?)
}

/// Admit one progress round atomically under Task authority. A replay keeps its
/// earlier grant even after spending the last allowance. Owner re-runs are
/// free but stay within the limit.
pub async fn grant_review_progress(
    db: &SqliteDb,
    task: &Task,
    review: &Review,
    limit: i64,
) -> Result<bool> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    db.fence_task_lease_in_tx(&mut tx, &task.id, "review progress round")
        .await?;
    let valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task t WHERE t.id=? AND t.version=? AND t.status=? AND t.deleted_at IS NULL AND ?=(SELECT r.id FROM review r WHERE r.task_id=t.id ORDER BY r.attempt_number DESC,r.id DESC LIMIT 1))")
        .bind(&task.id).bind(task.version).bind(&task.status).bind(&review.id).fetch_one(&mut *tx).await?;
    if !valid {
        return Err(DbError::VersionConflict);
    }
    let window: String = sqlx::query_scalar("SELECT COALESCE((SELECT window_id FROM task_budget WHERE task_id=? AND kind='review'),'initial')")
        .bind(&task.id).fetch_one(&mut *tx).await?;
    reset(&mut tx, &task.id, Kind::ReviewProgress.key(), &window).await?;
    if owner_requested_review_entry(&mut tx, &task.id, None, Some(&review.created_at)).await? {
        // An owner's re-run spends nothing, but it never exceeds the limit:
        // `review_progress = 0` stays disabled and a spent window stays spent.
        let spent: i64 =
            sqlx::query_scalar("SELECT spent FROM task_budget WHERE task_id=? AND kind=?")
                .bind(&task.id)
                .bind(Kind::ReviewProgress.key())
                .fetch_one(&mut *tx)
                .await?;
        tx.commit().await?;
        return Ok(allows_retry(limit, spent));
    }
    let receipt = format!("progress:{}", review.id);
    let paid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_budget_charge WHERE task_id=? AND kind=? AND window_id=? AND step_id=?)")
        .bind(&task.id).bind(Kind::ReviewProgress.key()).bind(&window).bind(&receipt).fetch_one(&mut *tx).await?;
    let granted = paid
        || charge(
            &mut tx,
            &task.id,
            Kind::ReviewProgress.key(),
            limit,
            &receipt,
        )
        .await?
        .charged;
    tx.commit().await?;
    Ok(granted)
}
/// A failed review-entry hook with no failed Review consumes one Review
/// attempt, owner-entered or not, exactly as base counted the bounce; only an
/// entry the owner asked for as a review re-run
/// ([`owner_requested_review_entry`]) consumes none. Its
/// receipt ([`entry_charge_step`]) and either the exhausted barrier or the
/// cleared barrier commit together; the bounce cascade reuses the receipt.
pub async fn failed_review_entry(
    db: &SqliteDb,
    task: &Task,
    limit: i64,
    hook_step: &str,
    started_at: &str,
) -> Result<(Task, bool)> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    db.fence_task_lease_in_tx(&mut tx, &task.id, "failed review entry budget")
        .await?;
    let current = db
        .get_task_in_tx(&mut tx, &task.id)
        .await?
        .ok_or(DbError::NotFound)?;
    if current.status != task.status {
        return Err(DbError::VersionConflict);
    }
    let spent_now = if owner_requested_review_entry(&mut tx, &task.id, None, None).await? {
        // The owner asked for this entry: it leaves the budget where it was.
        sqlx::query_scalar(
            "SELECT COALESCE((SELECT spent FROM task_budget WHERE task_id=? AND kind=?),0)",
        )
        .bind(&task.id)
        .bind(Kind::Review.key())
        .fetch_one(&mut *tx)
        .await?
    } else {
        charge(
            &mut tx,
            &task.id,
            Kind::Review.key(),
            limit,
            &entry_charge_step(hook_step),
        )
        .await?
        .spent
    };
    let allowed = allows_retry(limit, spent_now);
    let barrier = (!allowed).then(|| exhausted_review_barrier(&task.status, started_at));
    sqlx::query("UPDATE task SET entry_barrier_json=?,updated_at=?,version=version+1 WHERE id=?")
        .bind(barrier)
        .bind(crate::now_rfc3339())
        .bind(&task.id)
        .execute(&mut *tx)
        .await?;
    let updated = db
        .get_task_in_tx(&mut tx, &task.id)
        .await?
        .ok_or(DbError::NotFound)?;
    crate::task_condition::produce(&mut tx, &updated.id, crate::ConditionChange::Legacy).await?;
    tx.commit().await?;
    Ok((updated, allowed))
}
/// The entry barrier that parks a Task whose review entry budget ran out. The
/// reset-and-retry offers key on its `blocking_reason`.
pub fn exhausted_review_barrier(state: &str, started_at: &str) -> String {
    serde_json::to_string(&channel::EntryBarrier {
        state: state.into(),
        status: channel::BarrierStatus::Blocked,
        started_at: Some(Some(started_at.into())),
        updated_at: Some(Some(crate::now_rfc3339())),
        blocking_reason: Some(Some("review retry budget exhausted".into())),
        interrupted_at: None,
    })
    .expect("entry barrier serializes")
}
/// A pre-upgrade verdict, or a crash between verdict persistence and disposition,
/// is settled exactly once using the same charge point as a live verdict.
pub async fn reconcile_failed_review(db: &SqliteDb, task: &Task, review: &Review) -> Result<()> {
    if review.status != ReviewStatus::Failed {
        return Ok(());
    }
    let paid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_budget_charge c JOIN task_budget b USING(task_id,kind) WHERE c.task_id=? AND c.kind='review' AND c.window_id=b.window_id AND c.step_id=?)").bind(&task.id).bind(format!("review:{}",review.id)).fetch_one(db.pool()).await?;
    if paid {
        return Ok(());
    }
    let mut details = serde_json::from_str::<Value>(&review.step_results_json)
        .unwrap_or_else(|_| serde_json::json!({}));
    if !details.is_object() {
        details = serde_json::json!({});
    }
    if details.get("conformance").is_some_and(|c| {
        !c.is_null() && serde_json::from_value::<api_types::ReviewConformance>(c.clone()).is_err()
    }) {
        details
            .as_object_mut()
            .expect("normalized object")
            .remove("conformance");
    }
    let mut tx = crate::begin_immediate(db.pool()).await?;
    db.fence_task_lease_in_tx(&mut tx, &task.id, "review budget reconciliation")
        .await?;
    let owner:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM domain_event WHERE event_type='review.status_changed' AND actor_type='user' AND json_valid(payload_json) AND json_extract(payload_json,'$.review_id')=? AND json_extract(payload_json,'$.status')='failed')").bind(&review.id).fetch_one(&mut *tx).await?;
    let current = db
        .get_task_in_tx(&mut tx, &task.id)
        .await?
        .ok_or(DbError::NotFound)?;
    if current.status != task.status {
        return Err(DbError::VersionConflict);
    }
    review_verdict(&mut tx, &current, review, &review.status, &details, owner).await?;
    tx.commit().await?;
    Ok(())
}
pub async fn matches_spent(
    tx: &mut Transaction<'_, Sqlite>,
    task: &str,
    key: &str,
    expected: i64,
    window: &str,
) -> Result<bool> {
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_budget WHERE task_id=? AND kind=? AND spent=? AND window_id=?)").bind(task).bind(key).bind(expected).bind(window).fetch_one(&mut **tx).await?)
}
pub async fn cancelled_review_entry_allows_retry(
    db: &SqliteDb,
    task: &Task,
    gate: Option<&GateConfig>,
) -> Result<bool> {
    let cap = limit(task, Kind::GateRejection, None, gate)?
        .saturating_sub(1)
        .max(0);
    Ok(allows_retry(
        i64::from(cap),
        spent(db.pool(), &task.id, REVIEW_GATE_KEY).await?,
    ))
}
