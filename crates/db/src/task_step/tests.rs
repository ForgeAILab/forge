use super::*;
use crate::TaskRepo;
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
        kind: "cascade".into(),
        id: crate::new_uuid_v4(),
        task_id: task.into(),
        payload_json: "{}".into(),
        causation_step_id: None,
        causation_key: key.into(),
        chain_id: "chain".into(),
        chain_position: 1,
        expected_status: "todo".into(),
        expected_version: 1,
        expected_epoch: None,
        lane: "fast".into(),
        available_at: now_rfc3339(),
    }
}
fn later() -> String {
    (chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339()
}
#[tokio::test]
async fn workflow_snapshot_rolls_back_with_its_step_and_replays_without_orphans() {
    let db = fixture().await;
    let mut step = input("a", "frozen-workflow");
    step.payload_json = serde_json::json!({
        "workflow_ref": WorkflowReference::Definition("{\"states\":[]}".into())
    })
    .to_string();
    let mut tx = begin_immediate(db.pool()).await.unwrap();
    db.enqueue_step_in_tx(&mut tx, &step).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_step_workflow")
            .fetch_one(&mut *tx)
            .await
            .unwrap(),
        1
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_step_workflow")
            .fetch_one(db.pool())
            .await
            .unwrap(),
        0
    );
    assert!(db.task_steps("a").await.unwrap().is_empty());

    let id = db.enqueue_step(&step).await.unwrap();
    let stored = db.task_steps("a").await.unwrap().remove(0);
    let payload: serde_json::Value = serde_json::from_str(&stored.payload_json).unwrap();
    assert_eq!(payload["workflow_ref"]["kind"], "snapshot");
    assert_eq!(
        db.step_workflow(payload["workflow_ref"]["id"].as_str().unwrap())
            .await
            .unwrap(),
        "{\"states\":[]}"
    );
    step.payload_json = serde_json::json!({"workflow_ref": WorkflowReference::Definition("different workflow".into())}).to_string();
    assert_eq!(db.enqueue_step(&step).await.unwrap(), id);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_step_workflow")
            .fetch_one(db.pool())
            .await
            .unwrap(),
        1
    );
    step.id = crate::new_uuid_v4();
    step.causation_key = "same-definition".into();
    step.payload_json = serde_json::json!({"workflow_ref": WorkflowReference::Definition("{\"states\":[]}".into())}).to_string();
    db.enqueue_step(&step).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_step_workflow")
            .fetch_one(db.pool())
            .await
            .unwrap(),
        1
    );
}
#[tokio::test]
async fn fast_commands_do_not_hide_owner_actions_but_long_commands_do() {
    let db = fixture().await;
    let mut fast = input("a", "fast-command");
    fast.kind = "command".into();
    db.enqueue_step(&fast).await.unwrap();
    assert!(!db.entry_hooks_pending("a").await.unwrap());
    let mut long = input("a", "long-command");
    long.kind = "command".into();
    long.lane = "long".into();
    db.enqueue_step(&long).await.unwrap();
    assert!(db.entry_hooks_pending("a").await.unwrap());
}
#[tokio::test]
async fn enqueue_rolls_back_with_producing_cas_and_deduplicates() {
    let db = fixture().await;
    let i = input("a", "one");
    let mut tx = begin_immediate(db.pool()).await.unwrap();
    sqlx::query("UPDATE task SET status='planning',version=version+1 WHERE id='a' AND version=1")
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
    sqlx::query("UPDATE task SET status='planning',version=version+1 WHERE id='a' AND version=1")
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
async fn epoch_fence_tolerates_edits_and_markers_but_not_a_return_to_same_status() {
    let db = fixture().await;
    let id = db.enqueue_step(&input("a", "edit")).await.unwrap();
    let step = db
        .task_steps("a")
        .await
        .unwrap()
        .into_iter()
        .find(|s| s.id == id)
        .unwrap();
    assert_eq!(step.expected_epoch, 0);
    sqlx::query("UPDATE task SET title='edited',version=version+1 WHERE id='a'")
        .execute(db.pool())
        .await
        .unwrap();
    // A same-state recovery marker or reorder audit row is not an entry.
    sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,created_at) VALUES('marker','a','todo','todo','user','retry window reset',?)").bind(now_rfc3339()).execute(db.pool()).await.unwrap();
    sqlx::query("UPDATE task SET status='todo',version=version+1 WHERE id='a'")
        .execute(db.pool())
        .await
        .unwrap();
    assert!(db.step_entry_matches(&step).await.unwrap());
    // Leaving and returning, by any writer, is a new entry.
    for status in ["planning", "todo"] {
        sqlx::query("UPDATE task SET status=? WHERE id='a'")
            .bind(status)
            .execute(db.pool())
            .await
            .unwrap();
    }
    let epoch: i64 = sqlx::query_scalar("SELECT status_epoch FROM task WHERE id='a'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(epoch, 2);
    assert!(!db.step_entry_matches(&step).await.unwrap());
    // An in-transaction enqueue reads the epoch after the producing CAS.
    let mut tx = begin_immediate(db.pool()).await.unwrap();
    sqlx::query("UPDATE task SET status='planning' WHERE id='a'")
        .execute(&mut *tx)
        .await
        .unwrap();
    let mut produced = input("a", "produced");
    produced.expected_status = "planning".into();
    db.enqueue_step_in_tx(&mut tx, &produced).await.unwrap();
    tx.commit().await.unwrap();
    let produced = db
        .task_steps("a")
        .await
        .unwrap()
        .into_iter()
        .find(|s| s.causation_key == "produced")
        .unwrap();
    assert_eq!(produced.expected_epoch, 3);
    assert!(db.step_entry_matches(&produced).await.unwrap());
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
        db.prune_steps("2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z", 1000)
            .await
            .unwrap(),
        100
    );
    assert_eq!(db.task_steps("a").await.unwrap().len(), 5);
    assert_eq!(db.pending_steps("b").await.unwrap(), 1);
}
#[tokio::test]
async fn retention_keeps_unresolved_rows_longer_and_uses_indexes() {
    let db = fixture().await;
    let workflow = db.store_step_workflow("{\"states\":[]}").await.unwrap();
    for (key, status, completed) in [
        ("done-old", "done", "2000-01-01T00:00:00Z"),
        ("failed-recent", "failed", "2020-01-20T00:00:00Z"),
        ("parked-old", "parked", "2000-01-01T00:00:00Z"),
    ] {
        let mut value = input("a", key);
        value.chain_id = key.into();
        if status == "failed" {
            value.payload_json =
                serde_json::json!({"workflow_ref":{"kind":"snapshot","id":workflow}}).to_string();
        }
        db.enqueue_step(&value).await.unwrap();
        sqlx::query("UPDATE task_step SET status=?,completed_at=? WHERE causation_key=?")
            .bind(status)
            .bind(completed)
            .bind(key)
            .execute(db.pool())
            .await
            .unwrap();
    }
    sqlx::query("UPDATE task_step_workflow SET last_used_at='2000-01-01T00:00:00Z'")
        .execute(db.pool())
        .await
        .unwrap();
    // Seven-day cutoff for settled rows, thirty-day cutoff for unresolved.
    assert_eq!(
        db.prune_steps("2020-01-25T00:00:00Z", "2020-01-01T00:00:00Z", 100)
            .await
            .unwrap(),
        2
    );
    let left: Vec<String> = db
        .task_steps("a")
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.causation_key)
        .collect();
    assert_eq!(left, vec!["failed-recent".to_owned()]);
    // The failed row still references its definition.
    assert!(db.step_workflow(&workflow).await.is_ok());
    db.prune_steps("2020-01-25T00:00:00Z", "2020-02-01T00:00:00Z", 100)
        .await
        .unwrap();
    assert!(db.task_steps("a").await.unwrap().is_empty());
    assert!(db.step_workflow(&workflow).await.is_err());
    for (sql, index) in [
        (PRUNE_SETTLED, "task_step_settled"),
        (PRUNE_UNRESOLVED, "task_step_settled"),
        (PRUNE_WORKFLOWS, "task_step_workflow_ref"),
    ] {
        let explain = format!("EXPLAIN QUERY PLAN {sql}");
        let mut query = sqlx::query(&explain).bind("2020-01-01T00:00:00Z");
        if sql == PRUNE_WORKFLOWS {
            query = query.bind(100);
        } else {
            query = query.bind(now_rfc3339()).bind("[]").bind(100);
        }
        let plan: Vec<String> = query
            .fetch_all(db.pool())
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect();
        assert!(
            plan.iter().any(|detail| detail.contains(index)),
            "{index} not used: {plan:?}"
        );
    }
}
#[tokio::test]
async fn hook_retry_after_sleep_lands_for_a_live_local_owner() {
    let db = fixture().await;
    db.enqueue_step(&input("a", "sleep")).await.unwrap();
    let step = db
        .claim_step("owner", Some("a"), "2000-01-01T00:00:00+00:00")
        .await
        .unwrap()
        .unwrap();
    let activity = db.hold_task_step(&step);
    db.retry_step(&step, "transient after wake", &later())
        .await
        .unwrap();
    let row = &db.task_steps("a").await.unwrap()[0];
    assert_eq!(row.status, "pending");
    assert_eq!(row.last_error.as_deref(), Some("transient after wake"));
    drop(activity);
    // Without the local witness an expired owner is fenced out.
    let stale = db
        .claim_step("stale", Some("a"), "2000-01-01T00:00:00+00:00")
        .await
        .unwrap();
    assert!(stale.is_none(), "back-off keeps the step pending");
    sqlx::query("UPDATE task_step SET available_at='2000-01-01T00:00:00Z'")
        .execute(db.pool())
        .await
        .unwrap();
    let stale = db
        .claim_step("stale", Some("a"), "2000-01-01T00:00:00+00:00")
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        db.retry_step(&stale, "lost", &later()).await,
        Err(DbError::VersionConflict)
    ));
}
#[tokio::test]
async fn queued_admissions_count_only_imminent_role_entries() {
    let db = fixture().await;
    let now = now_rfc3339();
    for id in ["c", "d", "e", "f"] {
        sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES (?,'p',?,'todo',?,?)")
            .bind(id).bind(id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    }
    let role_entry = |task: &str, key: &str, lane: &str| {
        let mut value = input(task, key);
        value.payload_json = serde_json::json!({"admission_agent_id":"agent"}).to_string();
        value.lane = lane.into();
        value
    };
    // a: available head entry -> counts.
    db.enqueue_step(&role_entry("a", "head", "fast"))
        .await
        .unwrap();
    assert_eq!(db.queued_admissions("agent", "z").await.unwrap(), (1, 1));
    // b: a long-lane step inside its CI hook, with an entry queued behind it.
    db.enqueue_step(&role_entry("b", "ci", "long"))
        .await
        .unwrap();
    db.enqueue_step(&role_entry("b", "behind", "fast"))
        .await
        .unwrap();
    db.claim_step("ci-owner", Some("b"), &later())
        .await
        .unwrap()
        .unwrap();
    // g: an available long-lane head waiting for a long slot (CI first).
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES ('g','p','g','todo',?,?)")
        .bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    db.enqueue_step(&role_entry("g", "long-head", "long"))
        .await
        .unwrap();
    // c: in retry back-off; d: under its producer's reservation.
    let mut backoff = role_entry("c", "backoff", "fast");
    backoff.available_at = later();
    db.enqueue_step(&backoff).await.unwrap();
    let mut reserved = role_entry("d", "reserved", "fast");
    reserved.available_at = later();
    db.enqueue_step(&reserved).await.unwrap();
    assert_eq!(db.queued_admissions("agent", "z").await.unwrap(), (1, 1));
    // e: claimed fast entry; f: done, still leased for its inline dispatch.
    db.enqueue_step(&role_entry("e", "dispatching", "fast"))
        .await
        .unwrap();
    db.claim_step("e-owner", Some("e"), &later())
        .await
        .unwrap()
        .unwrap();
    db.enqueue_step(&role_entry("f", "committed", "fast"))
        .await
        .unwrap();
    let committed = db
        .claim_step("f-owner", Some("f"), &later())
        .await
        .unwrap()
        .unwrap();
    let mut tx = begin_immediate(db.pool()).await.unwrap();
    db.finish_step_in_tx(&mut tx, &committed, "done", None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(db.queued_admissions("agent", "z").await.unwrap(), (2, 2));
    db.release_step(&committed.id, "f-owner").await.unwrap();
    let mut hooks = role_entry("f", "hook-dispatch", "fast");
    hooks.kind = "hooks".into();
    hooks.payload_json =
        serde_json::json!({"admission_agent_id":"agent", "dispatch_index":0}).to_string();
    db.enqueue_step(&hooks).await.unwrap();
    let hook_step = db
        .claim_step("hook-owner", Some("f"), &later())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(db.queued_admissions("agent", "z").await.unwrap(), (3, 3));
    assert_eq!(db.queued_admissions("other", "a").await.unwrap(), (0, 2));
    db.start_hook(&hook_step, 0).await.unwrap();
    db.finish_hook(&hook_step, 0, "\"Ok\"").await.unwrap();
    assert_eq!(db.queued_admissions("agent", "z").await.unwrap(), (2, 2));
}
/// A cascade settles in its commit but keeps its lease until its owner
/// releases it. The entry it queued is already the Task's imminent
/// admission: hiding it for that window let a second waiter past a full
/// machine in the same dispatcher pass.
#[tokio::test]
async fn queued_admissions_count_an_entry_behind_a_settled_leased_step() {
    let db = fixture().await;
    let mut cascade = input("a", "cascade");
    cascade.payload_json = serde_json::json!({"admission_agent_id":"agent"}).to_string();
    db.enqueue_step(&cascade).await.unwrap();
    let settled = db
        .claim_step("owner", Some("a"), &later())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(db.queued_admissions("agent", "z").await.unwrap(), (1, 1));
    let mut tx = begin_immediate(db.pool()).await.unwrap();
    db.finish_step_in_tx(&mut tx, &settled, "done", None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut entry = input("a", "entry");
    entry.kind = "hooks".into();
    entry.payload_json =
        serde_json::json!({"admission_agent_id":"agent", "dispatch_index":0}).to_string();
    db.enqueue_step(&entry).await.unwrap();
    // Settled, lease not yet released.
    assert_eq!(db.queued_admissions("agent", "z").await.unwrap(), (1, 1));
    db.release_step(&settled.id, "owner").await.unwrap();
    assert_eq!(db.queued_admissions("agent", "z").await.unwrap(), (1, 1));
}
#[tokio::test]
async fn hook_and_script_checkpoints_reject_stale_owners_and_serialize_writers() {
    let db = fixture().await;
    let mut input = input("a", "hook");
    input.kind = "hooks".into();
    db.enqueue_step(&input).await.unwrap();
    let step = db
        .claim_step("owner", Some("a"), &later())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(db.start_hook(&step, 0).await.unwrap(), (None, false));
    assert_eq!(db.start_hook(&step, 0).await.unwrap(), (None, true));
    assert_eq!(
        db.start_hook_script(&step, 0, 0).await.unwrap(),
        (None, false)
    );
    assert_eq!(
        db.start_hook_script(&step, 0, 0).await.unwrap(),
        (None, true)
    );
    db.finish_hook_script(&step, 0, 0, "script-result")
        .await
        .unwrap();
    assert_eq!(
        db.start_hook_script(&step, 0, 0).await.unwrap(),
        (Some("script-result".into()), false)
    );
    db.record_hook_effect(&step, 0, "merge", "{\"Done\":true}")
        .await
        .unwrap();
    assert_eq!(
        db.hook_effect(&step, 0, "merge").await.unwrap().as_deref(),
        Some("{\"Done\":true}")
    );
    sqlx::query("UPDATE task_step SET lease_until='2000-01-01T00:00:00Z' WHERE id=?")
        .bind(&step.id)
        .execute(db.pool())
        .await
        .unwrap();
    let replacement = db
        .claim_step("replacement", Some("a"), &later())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        db.finish_hook(&step, 0, "old").await,
        Err(DbError::VersionConflict)
    ));
    db.enqueue_task_mutation(
        "a",
        crate::TaskMutation::Sql {
            change: crate::ConditionChange::Entry,
            task_id: "a".into(),
            query: "UPDATE task SET status='other',version=version+1 WHERE id=?".into(),
            arguments: vec![serde_json::json!("a")],
        },
    )
    .await
    .unwrap();
    assert_eq!(
        TaskRepo::get_by_id(&db, "a", false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "todo"
    );
    db.finish_hook_script(&replacement, 0, 0, "current")
        .await
        .unwrap();
    let mut tx = begin_immediate(db.pool()).await.unwrap();
    db.finish_step_in_tx(&mut tx, &replacement, "done", None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db.release_step(&replacement.id, "replacement")
        .await
        .unwrap();
    let next = db
        .claim_step("writer", Some("a"), &later())
        .await
        .unwrap()
        .unwrap();
    crate::task_writer::in_task_step(next.clone(), db.execute_task_mutation(&next))
        .await
        .unwrap();
    assert_eq!(
        TaskRepo::get_by_id(&db, "a", false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "other"
    );
}
#[tokio::test]
async fn durable_hook_upgrade_unblocks_legacy_running_barrier_without_moving_epoch() {
    let db = fixture().await;
    sqlx::query("UPDATE task SET entry_barrier_json=?,title='preserved',version=7 WHERE id='a'")
        .bind(serde_json::json!({"status":"running","state":"todo","started_at":"2026-10-01T00:00:00Z","infrastructure_attempts":2}).to_string()).execute(db.pool()).await.unwrap();
    let sql = include_str!("../../migrations/V202610040225__durable_hook_steps.sql")
        .split("-- Legacy running barriers")
        .nth(1)
        .unwrap();
    let sql = &sql[sql.find("UPDATE task SET").unwrap()..];
    sqlx::raw_sql(sql).execute(db.pool()).await.unwrap();
    let (title, version, epoch, raw): (String, i64, i64, String) = sqlx::query_as(
        "SELECT title,version,status_epoch,entry_barrier_json FROM task WHERE id='a'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(title, "preserved");
    assert_eq!(version, 8);
    assert_eq!(epoch, 0);
    let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(value["status"], "blocked");
    assert_eq!(value["infrastructure_attempts"], 2);
}

// A real upgrade from the pre-durable-hooks schema with in-flight step rows.
#[tokio::test]
async fn upgrade_preserves_in_flight_steps_and_converts_running_barrier() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let old = tempfile::TempDir::new().unwrap();
    for entry in std::fs::read_dir(&src).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".sql") && name.as_str() < "V202610040225" {
            std::fs::copy(entry.path(), old.path().join(&name)).unwrap();
        }
    }
    let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
    crate::run_migrations_from(&pool, old.path()).await.unwrap();
    let now = now_rfc3339();
    sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES ('p','p',?,?)")
        .bind(&now)
        .bind(&now)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at,entry_barrier_json) VALUES ('a','p','a','review',?,?,?)")
        .bind(&now).bind(&now).bind(serde_json::json!({"status":"running","state":"review"}).to_string()).execute(&pool).await.unwrap();
    for (id, seq, status, cause, lease) in [
        ("s1", 1, "done", None, Some("2099-01-01T00:00:00Z")),
        ("s2", 2, "claimed", Some("s1"), Some("2099-01-01T00:00:00Z")),
        ("s3", 3, "pending", Some("s2"), None),
    ] {
        sqlx::query("INSERT INTO task_step(id,task_id,seq,kind,payload_json,causation_step_id,causation_key,chain_id,chain_position,expected_status,expected_version,status,claimed_by,lease_until,available_at,created_at,updated_at,expected_epoch,lane) VALUES (?,'a',?,'cascade',?,?,?,'c',?,'review',1,?,?,?,?,?,?,4,'long')")
            .bind(id).bind(seq).bind(r#"{"workflow_ref":{"id":"wf"}}"#).bind(cause).bind(format!("k{seq}")).bind(seq).bind(status)
            .bind(lease.map(|_| "owner")).bind(lease).bind(&now).bind(&now).bind(&now).execute(&pool).await.unwrap();
    }
    crate::run_migrations_from(&pool, &src).await.unwrap();
    type UpgradedStep = (
        String,
        String,
        Option<String>,
        Option<String>,
        i64,
        String,
        Option<String>,
    );
    let rows: Vec<UpgradedStep> = sqlx::query_as(
        "SELECT id,status,causation_step_id,lease_until,expected_epoch,lane,workflow_ref_id FROM task_step ORDER BY seq").fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[1].2.as_deref(), Some("s1"));
    assert_eq!(rows[1].3.as_deref(), Some("2099-01-01T00:00:00Z"));
    assert_eq!(rows[2].4, 4);
    assert_eq!(rows[2].6.as_deref(), Some("wf"));
    let (barrier, ann): (String, Option<String>) =
        sqlx::query_as("SELECT entry_barrier_json,error_annotation FROM task WHERE id='a'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(barrier.contains("blocked"));
    assert!(ann.is_some_and(|a| a.contains("before_work_hook_failed")));
    let fk: Vec<(String,)> = sqlx::query_as("SELECT sql FROM sqlite_master WHERE name='task_step'")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert!(
        !fk[0].0.contains("task_step_new"),
        "self-reference not renamed: {}",
        fk[0].0
    );
}
#[tokio::test]
async fn retiring_a_queued_admission_kicks_capacity_waiters_without_an_execution() {
    let db = fixture().await;
    let mut first = input("a", "queued-role");
    first.kind = "command".into();
    first.payload_json =
        serde_json::json!({"operation":"reconcile_role","admission_agent_id":"agent-a"})
            .to_string();
    db.enqueue_step(&first).await.unwrap();
    db.schedule_wait("b", ("p", false), Some("agent-a"), None, None)
        .await
        .unwrap();
    sqlx::query("DELETE FROM task_schedule_dirty")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task_step SET status='done' WHERE id=?")
        .bind(&first.id)
        .execute(db.pool())
        .await
        .unwrap();
    let mut ids = db.dirty_schedule_tasks(100).await.unwrap();
    ids.sort();
    assert_eq!(ids, ["a", "b"]);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM execution")
            .fetch_one(db.pool())
            .await
            .unwrap(),
        0
    );
}
