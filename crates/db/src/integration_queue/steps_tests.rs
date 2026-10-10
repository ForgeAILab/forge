use super::tests::{admission, fixture, seed, FIXED_TIME};
use super::*;
use crate::{create_sqlite_pool, EnqueueTaskStep, TaskStepRepo};
use serde_json::json;

const NOW: &str = "2026-10-10T00:00:00Z";
const SHA_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SHA_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

async fn attempt(db: &SqliteDb, task: &str) -> IntegrationAttempt {
    let q = db
        .create_or_get_integration_queue("r", "main")
        .await
        .unwrap();
    db.admit_integration_attempt(admission(&q, task, &format!("entry-{task}")))
        .await
        .unwrap()
}
async fn reread(db: &SqliteDb, id: &str) -> IntegrationAttempt {
    db.integration_attempt(id).await.unwrap().unwrap()
}
/// Put the attempt where a head would be, without a worker.
async fn place(db: &SqliteDb, id: &str, state: &str, effect_seq: i64, generation: i64) {
    sqlx::query("UPDATE integration_attempt SET state=?,effect_seq=?,slot_generation=?,candidate_sha=?,target_tip_sha=?,revision=revision+1 WHERE id=?")
        .bind(state).bind(effect_seq).bind(generation).bind(SHA_A).bind(SHA_B).bind(id)
        .execute(db.pool()).await.unwrap();
}
fn ack(effect_seq: i64, generation: i64, action: &str, outcome: &str) -> Value {
    json!({"effect_seq":effect_seq,"generation":generation,"action":action,"outcome":outcome})
}
fn permit(a: &IntegrationAttempt) -> Value {
    json!({"candidate_sha":a.candidate_sha,"target_tip_sha":a.target_tip_sha,"task_ref":a.task_ref,"expected_epoch":a.expected_epoch,"slot_generation":a.slot_generation})
}
fn settle_write(
    a: &IntegrationAttempt,
    effect_seq: i64,
    generation: i64,
) -> IntegrationStepAckWrite {
    IntegrationStepAckWrite {
        attempt_id: a.id.clone(),
        generation,
        effect_seq,
        bind_generation: true,
        states: vec![IntegrationAttemptState::AwaitingTaskStep],
        ack: ack(effect_seq, generation, "settle", "permit"),
        permit: Some(permit(a)),
        acknowledged_at: NOW.into(),
    }
}
async fn write(db: &SqliteDb, w: &IntegrationStepAckWrite) -> Result<IntegrationStepAckOutcome> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    let outcome = acknowledge_integration_step_in_tx(&mut tx, w).await?;
    tx.commit().await?;
    Ok(outcome)
}
fn step(task: &str, key: &str, kind: &str, payload: Value) -> EnqueueTaskStep {
    EnqueueTaskStep {
        id: crate::new_uuid_v4(),
        task_id: task.into(),
        kind: kind.into(),
        payload_json: payload.to_string(),
        causation_step_id: None,
        causation_key: key.into(),
        chain_id: key.into(),
        chain_position: 1,
        expected_status: "merging".into(),
        expected_version: 1,
        expected_epoch: Some(0),
        lane: "fast".into(),
        available_at: FIXED_TIME.into(),
    }
}
async fn step_status(db: &SqliteDb, id: &str) -> String {
    sqlx::query_scalar("SELECT status FROM task_step WHERE id=?")
        .bind(id)
        .fetch_one(db.pool())
        .await
        .unwrap()
}
fn lease() -> String {
    (chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339()
}

#[tokio::test]
async fn the_ack_writer_is_a_cas_on_revision_generation_and_effect_seq() {
    let db = fixture().await;
    let a = attempt(&db, "a").await;
    // The step arrives before its attempt transition: nothing is written.
    place(&db, &a.id, "validating", 3, 7).await;
    let a = reread(&db, &a.id).await;
    assert!(matches!(
        write(&db, &settle_write(&a, 3, 7)).await.unwrap(),
        IntegrationStepAckOutcome::NotYet(_)
    ));
    assert_eq!(reread(&db, &a.id).await.revision, a.revision);
    place(&db, &a.id, "awaiting_task_step", 3, 7).await;
    let a = reread(&db, &a.id).await;
    // A permit that names another candidate, target, entry or slot is refused.
    for (field, value) in [
        ("candidate_sha", json!(SHA_B)),
        ("target_tip_sha", json!(SHA_A)),
        ("task_ref", json!("b")),
        ("expected_epoch", json!(9)),
        ("slot_generation", json!(8)),
    ] {
        let mut w = settle_write(&a, 3, 7);
        w.permit.as_mut().unwrap()[field] = value;
        assert!(
            matches!(write(&db, &w).await, Err(DbError::Check(_))),
            "{field}"
        );
    }
    // An acknowledgment that does not name its own effect is malformed.
    let mut w = settle_write(&a, 3, 7);
    w.ack["effect_seq"] = json!(2);
    assert!(matches!(write(&db, &w).await, Err(DbError::Check(_))));
    // A moved effect_seq or generation finishes without a write.
    for (seq, generation) in [(2, 7), (4, 7), (3, 6), (3, 8)] {
        assert!(matches!(
            write(&db, &settle_write(&a, seq, generation))
                .await
                .unwrap(),
            IntegrationStepAckOutcome::Stale(_)
        ));
    }
    assert_eq!(reread(&db, &a.id).await, a);
    let IntegrationStepAckOutcome::Written(written) =
        write(&db, &settle_write(&a, 3, 7)).await.unwrap()
    else {
        panic!("due acknowledgment is written");
    };
    assert_eq!(written, reread(&db, &a.id).await);
    assert_eq!(written.revision, a.revision + 1);
    assert_eq!(written.acknowledged_at.as_deref(), Some(NOW));
    assert_eq!(written.permit_json, Some(permit(&a)));
    // Duplicate delivery: one acknowledgment, no second write.
    assert!(matches!(
        write(&db, &settle_write(&a, 3, 7)).await.unwrap(),
        IntegrationStepAckOutcome::AlreadyAcknowledged(_)
    ));
    assert_eq!(reread(&db, &a.id).await.revision, written.revision);
    // The permit the step wrote is the one storage accepts for `ready_ff`.
    let mut ready = written.clone();
    ready.state = IntegrationAttemptState::ReadyFf;
    db.transition_integration_attempt(ready).await.unwrap();
    // `result` does not bind the generation: the lease may have moved.
    place(&db, &a.id, "applied", 3, 9).await;
    let result = IntegrationStepAckWrite {
        bind_generation: false,
        states: vec![IntegrationAttemptState::Applied],
        ack: ack(3, 7, "result", "done"),
        permit: None,
        ..settle_write(&a, 3, 7)
    };
    assert!(matches!(
        write(&db, &result).await.unwrap(),
        IntegrationStepAckOutcome::Written(_)
    ));
    assert_eq!(
        reread(&db, &a.id).await.effect_ack_json.unwrap()["action"],
        "result"
    );
}

#[tokio::test]
async fn the_ack_and_the_task_write_commit_or_roll_back_together() {
    let db = fixture().await;
    let a = attempt(&db, "a").await;
    place(&db, &a.id, "awaiting_task_step", 1, 1).await;
    let a = reread(&db, &a.id).await;
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    sqlx::query("UPDATE task SET title='written' WHERE id='a'")
        .execute(&mut *tx)
        .await
        .unwrap();
    acknowledge_integration_step_in_tx(&mut tx, &settle_write(&a, 1, 1))
        .await
        .unwrap();
    drop(tx);
    let title: String = sqlx::query_scalar("SELECT title FROM task WHERE id='a'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(title, "a");
    assert_eq!(reread(&db, &a.id).await, a);
}

#[tokio::test]
async fn a_permit_is_revoked_only_while_no_fast_forward_was_committed() {
    let db = fixture().await;
    let a = attempt(&db, "a").await;
    place(&db, &a.id, "awaiting_task_step", 1, 1).await;
    let a = reread(&db, &a.id).await;
    write(&db, &settle_write(&a, 1, 1)).await.unwrap();
    let revoke = |seq: i64| {
        let db = db.clone();
        let id = a.id.clone();
        async move {
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            let revoked = revoke_integration_permit_in_tx(&mut tx, &id, seq, NOW)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            revoked
        }
    };
    assert!(!revoke(2).await, "another effect_seq is not this permit");
    sqlx::query("UPDATE integration_attempt SET state='ff_inflight' WHERE id=?")
        .bind(&a.id)
        .execute(db.pool())
        .await
        .unwrap();
    assert!(
        !revoke(1).await,
        "a committed fast-forward keeps its permit"
    );
    assert!(reread(&db, &a.id).await.permit_json.is_some());
    sqlx::query("UPDATE integration_attempt SET state='ready_ff' WHERE id=?")
        .bind(&a.id)
        .execute(db.pool())
        .await
        .unwrap();
    assert!(revoke(1).await);
    let revoked = reread(&db, &a.id).await;
    assert!(revoked.permit_json.is_none() && revoked.acknowledged_at.is_none());
    assert!(!revoke(1).await, "idempotent");
}

#[tokio::test]
async fn task_cancel_flags_a_cancellable_attempt_and_waits_only_behind_a_protected_step() {
    let db = fixture().await;
    let request = |task: &'static str| {
        let db = db.clone();
        async move {
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            let outcome = request_task_integration_cancel_in_tx(&mut tx, task, NOW)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            outcome
        }
    };
    assert_eq!(request("a").await, TaskIntegrationCancel::NoAttempt);
    let a = attempt(&db, "a").await;
    for state in [
        "queued",
        "path_wait",
        "validating",
        "rebasing",
        "checking",
        "awaiting_task_step",
        "ready_ff",
        "ejected",
        "needs_review",
        "parked",
    ] {
        sqlx::query("UPDATE integration_attempt SET state=?,cancel_requested_at=NULL,permit_json=NULL WHERE id=?")
            .bind(state).bind(&a.id).execute(db.pool()).await.unwrap();
        assert_eq!(
            request("a").await,
            TaskIntegrationCancel::Requested,
            "{state}"
        );
        let flagged = reread(&db, &a.id).await;
        assert_eq!(flagged.cancel_requested_at.as_deref(), Some(NOW), "{state}");
        // A second request keeps the first time and writes nothing.
        assert_eq!(request("a").await, TaskIntegrationCancel::Requested);
        assert_eq!(reread(&db, &a.id).await.revision, flagged.revision);
    }
    let protected = db
        .enqueue_step(&step("a", "unprotected", "integration", json!({})))
        .await
        .unwrap();
    for state in ["ff_inflight", "reconciling", "applied", "quarantined"] {
        sqlx::query("UPDATE integration_attempt SET state=?,cancel_requested_at=NULL WHERE id=?")
            .bind(state)
            .bind(&a.id)
            .execute(db.pool())
            .await
            .unwrap();
        // Nothing drives the row (a shadow or imported attempt): the Cancel
        // is not held back and nothing is written.
        sqlx::query("UPDATE task_step SET integration_started_at=NULL WHERE id=?")
            .bind(&protected)
            .execute(db.pool())
            .await
            .unwrap();
        let before = reread(&db, &a.id).await;
        assert_eq!(
            request("a").await,
            TaskIntegrationCancel::Undriven,
            "{state}"
        );
        // With the protected `result` step alive the Cancel waits behind it.
        sqlx::query("UPDATE task_step SET integration_started_at=? WHERE id=?")
            .bind(NOW)
            .bind(&protected)
            .execute(db.pool())
            .await
            .unwrap();
        assert_eq!(
            request("a").await,
            TaskIntegrationCancel::Protected,
            "{state}"
        );
        assert_eq!(reread(&db, &a.id).await, before, "{state}");
    }
}

#[tokio::test]
async fn a_preempting_command_drops_unprotected_integration_steps_and_waits_behind_a_protected_one()
{
    let db = fixture().await;
    let unprotected = db
        .enqueue_step(&step(
            "a",
            "integration:x:1:settle",
            "integration",
            json!({}),
        ))
        .await
        .unwrap();
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    let mut result = step("a", "integration:x:1:result", "integration", json!({}));
    result.available_at = "2099-01-01T00:00:00Z".into();
    let protected = db
        .enqueue_protected_integration_step_in_tx(&mut tx, &result)
        .await
        .unwrap();
    // Idempotent on the causation key: the same row, still protected.
    assert_eq!(
        db.enqueue_protected_integration_step_in_tx(&mut tx, &result)
            .await
            .unwrap(),
        protected
    );
    // Only an integration step can be protected at enqueue.
    assert!(matches!(
        db.enqueue_protected_integration_step_in_tx(
            &mut tx,
            &step("a", "other", "command", json!({}))
        )
        .await,
        Err(DbError::Check(_))
    ));
    tx.commit().await.unwrap();
    let cancel = db
        .enqueue_step(&step("a", "cancel", "command", json!({"preempt":true})))
        .await
        .unwrap();
    assert_eq!(step_status(&db, &unprotected).await, "superseded");
    assert_eq!(step_status(&db, &protected).await, "pending");
    // The protected step is not yet due; the later Cancel still cannot be
    // claimed ahead of it.
    assert!(db
        .claim_step("w", Some("a"), &lease())
        .await
        .unwrap()
        .is_none());
    assert!(!db.ready_integration_step("a", "missing").await.unwrap());
    assert!(db
        .ready_integration_step("a", "integration:x:1:result")
        .await
        .unwrap());
    let claimed = db
        .claim_step("w", Some("a"), &lease())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.id, protected);
    assert!(!claimed.entry_fenced);
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    db.finish_step_in_tx(&mut tx, &claimed, "done", None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db.release_step(&claimed.id, "w").await.unwrap();
    assert_eq!(
        db.claim_step("w2", Some("a"), &lease())
            .await
            .unwrap()
            .unwrap()
            .id,
        cancel
    );
}

#[tokio::test]
async fn the_step_kind_migration_preserves_rows_indexes_triggers_and_dependents() {
    const NEW: &str = "V202610100137__integration_task_step.sql";
    let dir = tempfile::tempdir().unwrap();
    let source = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"));
    for file in std::fs::read_dir(source).unwrap() {
        let file = file.unwrap();
        if file.file_name().to_str().unwrap() != NEW {
            std::fs::copy(file.path(), dir.path().join(file.file_name())).unwrap();
        }
    }
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    crate::run_migrations_from(&pool, dir.path()).await.unwrap();
    seed(&pool).await;
    let db = SqliteDb::new(pool.clone());
    let schema = |pool: crate::SqlitePool| async move {
        sqlx::query_as::<_, (String, String, Option<String>)>("SELECT type,name,sql FROM sqlite_schema WHERE tbl_name='task_step' AND name NOT LIKE 'sqlite_autoindex%' ORDER BY type,name")
            .fetch_all(&pool)
            .await
            .unwrap()
    };
    let before_schema = schema(pool.clone()).await;
    assert!(
        db.enqueue_step(&step("a", "refused", "integration", json!({})))
            .await
            .is_err(),
        "the old CHECK refuses the kind"
    );
    let parent = db
        .enqueue_step(&step(
            "a",
            "parent",
            "hooks",
            json!({"workflow_ref":{"kind":"snapshot","id":"wf"}}),
        ))
        .await
        .unwrap();
    let mut child = step("a", "child", "command", json!({"preempt":false}));
    child.causation_step_id = Some(parent.clone());
    let child = db.enqueue_step(&child).await.unwrap();
    sqlx::query("UPDATE task_step SET status='claimed',claimed_by='owner',lease_until='2099-01-01T00:00:00Z',attempts=3,last_error='e',result_json='{}',priority=1,preempt_requested_at=?,integration_started_at=?,entry_fenced=0 WHERE id=?")
        .bind(FIXED_TIME).bind(FIXED_TIME).bind(&child).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO task_hook_checkpoint(step_id,hook_index,started_at) VALUES(?,0,?)")
        .bind(&parent)
        .bind(FIXED_TIME)
        .execute(&pool)
        .await
        .unwrap();
    let rows = |pool: crate::SqlitePool| async move {
        sqlx::query_scalar::<_, String>("SELECT json_group_array(json_array(id,task_id,seq,kind,payload_json,causation_step_id,causation_key,chain_id,chain_position,expected_status,expected_version,status,claimed_by,lease_until,available_at,attempts,last_error,created_at,updated_at,completed_at,result_json,priority,preempt_requested_at,integration_started_at,expected_epoch,lane,entry_fenced,workflow_ref_id)) FROM (SELECT * FROM task_step ORDER BY task_id,seq)")
            .fetch_one(&pool)
            .await
            .unwrap()
    };
    let before = rows(pool.clone()).await;
    std::fs::copy(source.join(NEW), dir.path().join(NEW)).unwrap();
    crate::run_migrations_from(&pool, dir.path()).await.unwrap();
    assert_eq!(rows(pool.clone()).await, before);
    let after_schema = schema(pool.clone()).await;
    assert_eq!(before_schema.len(), after_schema.len());
    for (old, new) in before_schema.iter().zip(&after_schema) {
        assert_eq!((&old.0, &old.1), (&new.0, &new.1));
        if old.0 != "table" {
            assert_eq!(old.2, new.2, "{}", old.1);
        }
    }
    let checkpoints: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_hook_checkpoint")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(checkpoints, 1);
    let violations = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert!(violations.is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    db.enqueue_step(&step("a", "accepted", "integration", json!({})))
        .await
        .unwrap();
    // Replay is a no-op.
    crate::run_migrations_from(&pool, dir.path()).await.unwrap();
}
