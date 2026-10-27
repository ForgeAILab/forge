//! Budget policy, receipts, review windows, and retry accounting tests.
use super::review::{coordination_root_in_tx, open_recovery_episode_in_tx};
use super::*;
use crate::TaskRepo;
use serde_json::json;

async fn fixture() -> (SqliteDb, Task) {
    let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
    crate::run_migrations(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO project(id,name,created_at,updated_at) VALUES('p','Project','now','now')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t','p','Task','review','now','now')").execute(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    let task = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
    (db, task)
}

/// The re-plan limit is fixed: no `retry_budgets` value, Task-wide or
/// per state, changes it, and what is projected is counted against it.
#[tokio::test]
async fn the_replan_limit_is_fixed_and_projected_only_once_spent() {
    let (db, mut task) = fixture().await;
    task.task_state_config = Some(
        json!({"retry_budgets":{"replan":1},"in_progress":{"retry_budgets":{"replan":9}}})
            .to_string(),
    );
    let merged = json!({"retry_budgets":{"replan":9}});
    assert_eq!(limit(&task, Kind::Replan, None, None).unwrap(), 5);
    assert_eq!(limit(&task, Kind::Replan, Some(&merged), None).unwrap(), 5);
    assert!(!Kind::PERSISTED.contains(&Kind::Replan));
    let workflow = WorkflowDefinition::default();
    let before = projection(
        &task,
        &workflow,
        &load(db.pool(), &task.id).await.unwrap(),
        3,
    )
    .unwrap();
    assert!(!before.contains_key("replan"), "{before:?}");
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    assert!(
        charge(&mut tx, &task.id, "replan", 5, "replan:child:park")
            .await
            .unwrap()
            .charged
    );
    tx.commit().await.unwrap();
    assert!(
        receipt_recorded(db.pool(), &task.id, "replan", "replan:child:park")
            .await
            .unwrap()
    );
    assert!(
        !receipt_recorded(db.pool(), &task.id, "replan", "replan:child:other")
            .await
            .unwrap()
    );
    let after = projection(
        &task,
        &workflow,
        &load(db.pool(), &task.id).await.unwrap(),
        3,
    )
    .unwrap();
    assert_eq!(after.get("replan"), Some(&4), "{after:?}");
}

#[tokio::test]
async fn every_persisted_kind_charges_blocks_replays_and_resets() {
    let (db, task) = fixture().await;
    let mut keys = Kind::PERSISTED
        .into_iter()
        .map(|k| k.key().to_owned())
        .collect::<Vec<_>>();
    keys.push(gate_key("planning"));
    for key in keys {
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        let first = charge(&mut tx, &task.id, &key, 2, "first").await.unwrap();
        assert!(first.charged);
        assert_eq!(first.remaining, 1);
        let duplicate = charge(&mut tx, &task.id, &key, 2, "first").await.unwrap();
        assert!(!duplicate.charged);
        assert_eq!(duplicate.spent, 1);
        let second = charge(&mut tx, &task.id, &key, 2, "second").await.unwrap();
        assert!(second.charged);
        assert_eq!(second.remaining, 0);
        let blocked = charge(&mut tx, &task.id, &key, 2, "third").await.unwrap();
        assert!(!blocked.charged);
        assert_eq!(blocked.spent, 2);
        reset(&mut tx, &task.id, &key, &format!("natural-window:{key}"))
            .await
            .unwrap();
        assert!(
            charge(&mut tx, &task.id, &key, 2, "first")
                .await
                .unwrap()
                .charged
        );
        // Replaying the reset must not refund a charge in that window.
        reset(&mut tx, &task.id, &key, &format!("natural-window:{key}"))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(spent(db.pool(), &task.id, &key).await.unwrap(), 1);
    }
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    reset_all(&mut tx, &task.id, "explicit-reset")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(load(db.pool(), &task.id)
        .await
        .unwrap()
        .values()
        .all(|n| *n == 0));
}

#[tokio::test]
async fn consuming_write_rollback_refunds_charge_and_owner_sendback_is_free() {
    let (db, task) = fixture().await;
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    charge(&mut tx, &task.id, Kind::Execution.key(), 3, "failure")
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        spent(db.pool(), &task.id, Kind::Execution.key())
            .await
            .unwrap(),
        0
    );
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    for state in ["review", "planning", "merging"] {
        transition(
            &mut tx,
            &task.id,
            state,
            true,
            &api_types::TransitionBridge::new(api_types::TransitionBridgeKind::GateRejected),
            true,
            state,
            None,
        )
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    assert!(load(db.pool(), &task.id)
        .await
        .unwrap()
        .values()
        .all(|n| *n == 0));
}

#[tokio::test]
async fn execution_spending_survives_review_lap_and_resets_only_explicitly() {
    let (db, task) = fixture().await;
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    charge(&mut tx, &task.id, Kind::Execution.key(), 3, "failure")
        .await
        .unwrap();
    sqlx::query("UPDATE task SET status='in_progress' WHERE id='t'")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE task SET status='review' WHERE id='t'")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        spent(db.pool(), &task.id, Kind::Execution.key())
            .await
            .unwrap(),
        1
    );
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    transition(
        &mut tx,
        &task.id,
        "review",
        true,
        &api_types::TransitionBridge::recovery("retry", true),
        false,
        "reset",
        None,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        spent(db.pool(), &task.id, Kind::Execution.key())
            .await
            .unwrap(),
        0
    );
}

async fn log_review_entry(
    db: &SqliteDb,
    id: &str,
    epoch: i64,
    at: &str,
    by: Option<api_types::Actor>,
) {
    let bridge = by
        .map(|actor| api_types::TransitionBridge::review_rerun(&actor))
        .unwrap_or_default();
    sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,created_at,bridge_kind,bridge_payload,status_epoch) VALUES(?,'t','review','review','system','entry',?,?,?,?)")
        .bind(id)
        .bind(at)
        .bind(bridge.bridge_kind.map(api_types::TransitionBridgeKind::as_str))
        .bind(bridge.bridge_payload.map(|payload| payload.to_string()))
        .bind(epoch)
        .execute(db.pool())
        .await
        .unwrap();
}
async fn review_spent(db: &SqliteDb) -> (i64, i64) {
    (
        spent(db.pool(), "t", Kind::Review.key()).await.unwrap(),
        spent(db.pool(), "t", REVIEW_GATE_KEY).await.unwrap(),
    )
}

// Every charge a review entry can cause is skipped when the owner asked
// for that entry as a review re-run, and only then: a failing reviewer and
// the bounce out of `review` here; the failed entry hook asks the same
// question under its step lease (services `rerun_review` tests).
#[tokio::test]
async fn an_owner_requested_review_rerun_entry_spends_nothing_and_any_other_entry_spends() {
    let (db, task) = fixture().await;
    let owner = api_types::Actor::user(api_types::UserActionSource::Api);
    let workflow = api_types::Actor::system(api_types::SystemComponent::Workflow);
    sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES('e','t','coder','completed','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')").execute(db.pool()).await.unwrap();
    let fail = |id: &'static str, attempt: i64, at: &'static str| {
        let db = &db;
        let task = &task;
        async move {
            sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,created_at,updated_at) VALUES(?,'t','e',?,'running','{}',?,?,?)").bind(id).bind(attempt).bind(at).bind(at).bind(at).execute(db.pool()).await.unwrap();
            let review = crate::ReviewRepo::get_by_id(db, id).await.unwrap().unwrap();
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            review_verdict(
                &mut tx,
                task,
                &review,
                &ReviewStatus::Failed,
                &json!({}),
                false,
            )
            .await
            .unwrap();
            sqlx::query("UPDATE review SET status='failed' WHERE id=?")
                .bind(id)
                .execute(&mut *tx)
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }
    };
    let bounce = |log: &'static str| {
        let db = &db;
        async move {
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            transition(
                &mut tx,
                "t",
                "review",
                false,
                &Default::default(),
                true,
                log,
                None,
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();
        }
    };

    // The owner's re-runs: three entries, each fails every way it can.
    for (n, (entry, review, at, out)) in [
        ("o1", "r1", "2026-01-01T00:00:01Z", "x1"),
        ("o2", "r2", "2026-01-01T00:00:02Z", "x2"),
        ("o3", "r3", "2026-01-01T00:00:03Z", "x3"),
    ]
    .into_iter()
    .enumerate()
    {
        log_review_entry(&db, entry, n as i64 + 1, at, Some(owner.clone())).await;
        fail(review, n as i64 + 1, at).await;
        bounce(out).await;
        assert_eq!(review_spent(&db).await, (0, 0), "owner re-run {entry}");
    }

    // A marker written while the Task sits in `review` is not an entry.
    sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,created_at,bridge_kind,bridge_payload) VALUES('m','t','review','review','user','note','2026-01-01T00:00:04Z','recovery','{\"verb\":\"retry\"}')").execute(db.pool()).await.unwrap();
    bounce("x4").await;
    assert_eq!(review_spent(&db).await, (0, 0));

    // A Review the entry did not open is not covered by it.
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    assert!(
        !owner_requested_review_entry(&mut tx, "t", None, Some("2026-01-01T00:00:00Z"))
            .await
            .unwrap()
    );
    assert!(
        owner_requested_review_entry(&mut tx, "t", None, Some("2026-01-01T00:00:03Z"))
            .await
            .unwrap()
    );
    drop(tx);

    // The same re-run asked for by the workflow itself spends, everywhere.
    log_review_entry(&db, "s1", 4, "2026-01-01T00:00:05Z", Some(workflow)).await;
    fail("r4", 4, "2026-01-01T00:00:05Z").await;
    assert_eq!(review_spent(&db).await, (1, 0), "the reviewer's verdict");
    bounce("x5").await;
    assert_eq!(review_spent(&db).await, (1, 1), "the bounce's gate debit");
    // And so does an ordinary entry.
    log_review_entry(&db, "a1", 5, "2026-01-01T00:00:06Z", None).await;
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    assert!(!owner_requested_review_entry(&mut tx, "t", None, None)
        .await
        .unwrap());
}

#[tokio::test]
async fn review_two_failed_verdicts_allow_one_bounce_and_owner_finding_is_free() {
    let (db, task) = fixture().await;
    let workflow = json!({"roles":[],"states":[{"name":"review","kind":"gate","column":"Review","display_name":"Review","role":null,"hooks":{},"config":{},"gate_config":{"max_rejections":2}}]});
    sqlx::query("UPDATE project SET workflow_definition=? WHERE id='p'")
        .bind(workflow.to_string())
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES('e','t','coder','completed','now','now')").execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,created_at,updated_at) VALUES('r','t','e',1,'running','{}','now','now','now')").execute(db.pool()).await.unwrap();
    let review = crate::ReviewRepo::get_by_id(&db, "r")
        .await
        .unwrap()
        .unwrap();
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    review_verdict(
        &mut tx,
        &task,
        &review,
        &ReviewStatus::Failed,
        &json!({}),
        true,
    )
    .await
    .unwrap();
    let conformance = api_types::ReviewConformance {
        status: api_types::ConformanceStatus::Failed,
        reason: Some("owner access".into()),
        assessment: Some(api_types::ReviewAssessment {
            result: api_types::ReviewResult::Fail,
            reason: "owner access".into(),
            fixable_by: api_types::FixableBy::Owner,
            repeat: false,
            findings: Vec::new(),
            resolved: Vec::new(),
            report: String::new(),
        }),
        ..Default::default()
    };
    review_verdict(
        &mut tx,
        &task,
        &review,
        &ReviewStatus::Failed,
        &json!({"conformance":conformance}),
        false,
    )
    .await
    .unwrap();
    let untouched: i64 = sqlx::query_scalar(
        "SELECT COALESCE((SELECT spent FROM task_budget WHERE task_id='t' AND kind='review'),0)",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(untouched, 0);
    let mut review = review.clone();
    review.id = "r1".into();
    review.attempt_number = 2;
    sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,created_at,updated_at) VALUES('r1','t','e',2,'running','{}','now','now','now')").execute(&mut *tx).await.unwrap();
    review_verdict(
        &mut tx,
        &task,
        &review,
        &ReviewStatus::Failed,
        &json!({}),
        false,
    )
    .await
    .unwrap();
    let first: i64 =
        sqlx::query_scalar("SELECT spent FROM task_budget WHERE task_id='t' AND kind='review'")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(remaining(2, first), 1);
    assert!(allows_retry(2, first));
    let mut second = review.clone();
    second.id = "r2".into();
    second.attempt_number = 3;
    sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,created_at,updated_at) VALUES('r2','t','e',3,'running','{}','now','now','now')").execute(&mut *tx).await.unwrap();
    review_verdict(
        &mut tx,
        &task,
        &second,
        &ReviewStatus::Failed,
        &json!({}),
        false,
    )
    .await
    .unwrap();
    let second_spent: i64 =
        sqlx::query_scalar("SELECT spent FROM task_budget WHERE task_id='t' AND kind='review'")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(remaining(2, second_spent), 0);
    assert!(!allows_retry(2, second_spent));
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn exhausting_verdict_opens_one_recovery_episode() {
    let (db, task) = fixture().await;
    let key = Kind::AutomaticReviewRecovery.key();
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    let episode = "SELECT window_id,spent FROM task_budget WHERE task_id='t' AND kind='automatic_review_recovery'";
    assert!(
        charge(&mut tx, &task.id, key, 1, "attempt")
            .await
            .unwrap()
            .charged
    );
    open_recovery_episode_in_tx(&mut tx, &task.id, "r2")
        .await
        .unwrap();
    let opened: (String, i64) = sqlx::query_as(episode).fetch_one(&mut *tx).await.unwrap();
    assert_eq!(opened, ("exhaustion:r2".to_owned(), 0));
    // Re-resolving the same exhausted verdict keeps the episode's spending.
    assert!(
        charge(&mut tx, &task.id, key, 1, "recovery")
            .await
            .unwrap()
            .charged
    );
    open_recovery_episode_in_tx(&mut tx, &task.id, "r2")
        .await
        .unwrap();
    let kept: (String, i64) = sqlx::query_as(episode).fetch_one(&mut *tx).await.unwrap();
    assert_eq!(kept, ("exhaustion:r2".to_owned(), 1));
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn resolver_honors_task_wide_then_supplied_state_config_then_gate() {
    let (_db, mut task) = fixture().await;
    let gate = api_types::GateConfig {
        reject_target: None,
        max_rejections: Some(2),
        approve_label: None,
        reject_label: None,
        requires_user_approval: None,
        optional_when_unassigned: None,
    };
    assert_eq!(limit(&task, Kind::Review, None, Some(&gate)).unwrap(), 2);
    // A per-state value is only seen where the step supplies its merged config.
    task.task_state_config = Some(json!({"review":{"retry_budgets":{"review":4}}}).to_string());
    assert_eq!(limit(&task, Kind::Review, None, Some(&gate)).unwrap(), 2);
    let merged = json!({"retry_budgets":{"review":4}});
    assert_eq!(
        limit(&task, Kind::Review, Some(&merged), Some(&gate)).unwrap(),
        4
    );
    // The web editor's Task-wide save wins over every per-state value.
    task.task_state_config = Some(
        json!({"retry_budgets":{"review":1},"review":{"retry_budgets":{"review":4}}}).to_string(),
    );
    assert_eq!(
        limit(&task, Kind::Review, Some(&merged), Some(&gate)).unwrap(),
        1
    );
    assert_eq!(limit(&task, Kind::Review, None, Some(&gate)).unwrap(), 1);
}

#[test]
fn ci_triage_has_no_budget_kind() {
    // The public triage mode remains a Project setting. It is never a
    // retry budget: durable check request keys bound replay instead.
    assert!(serde_json::from_str::<Kind>(r#""review_ci_triage""#).is_err());
}

#[test]
fn correction_invocation_has_two_turns_and_new_invocation_restores_allowance() {
    let mut invocation = Invocation::new(Kind::ReportCorrection);
    assert_eq!(invocation.remaining(), 2);
    assert!(invocation.consume());
    assert_eq!(invocation.remaining(), 1);
    assert!(invocation.consume());
    assert!(!invocation.consume());
    assert_eq!(Invocation::new(Kind::ReportCorrection).remaining(), 2);
}
#[tokio::test]
async fn budget_review_progress_resolves_and_projects_in_review_window_and_verify_is_persisted() {
    let (db, mut task) = fixture().await;
    let workflow = WorkflowDefinition { states: vec![api_types::StateDefinition {
        name:"review".into(), kind:api_types::StateKind::Gate, config:json!({"retry_budgets":{"review_progress":4}}),
        ..serde_json::from_value(json!({"name":"review","kind":"gate","column":"Review","display_name":"Review","hooks":{},"config":{}})).unwrap()
    }], ..Default::default() };
    let mut state = workflow.states[0].clone();
    state.config = json!({});
    let merged = state_config(&state, &json!({"retry_budgets":{"review_progress":2},"default_review_config":{"retry_budgets":{"review":7}}}).to_string());
    assert_eq!(merged["retry_budgets"]["review"], 7);
    assert_eq!(merged["retry_budgets"]["review_progress"], 2);
    let gate: GateConfig = serde_json::from_value(json!({"max_rejections":99})).unwrap();
    assert_eq!(
        limit(&task, Kind::ReviewProgress, None, Some(&gate)).unwrap(),
        2
    );
    assert_eq!(
        limit(
            &task,
            Kind::ReviewProgress,
            Some(&workflow.states[0].config),
            Some(&gate)
        )
        .unwrap(),
        4
    );
    task.task_state_config = Some(json!({"retry_budgets":{"review_progress":0}}).to_string());
    assert_eq!(
        limit(
            &task,
            Kind::ReviewProgress,
            Some(&workflow.states[0].config),
            None
        )
        .unwrap(),
        0
    );
    task.task_state_config = None;
    let projection = projection(
        &task,
        &workflow,
        &HashMap::from([("review_progress".into(), 1)]),
        0,
    )
    .unwrap();
    assert_eq!(projection.get("review_progress"), Some(&3));
    assert!(!Kind::ReviewVerifyRerun.is_volatile());
    assert!(Kind::PERSISTED.contains(&Kind::ReviewVerifyRerun));
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    assert!(
        charge(
            &mut tx,
            &task.id,
            Kind::ReviewVerifyRerun.key(),
            1,
            "verify"
        )
        .await
        .unwrap()
        .charged
    );
    assert!(
        !charge(
            &mut tx,
            &task.id,
            Kind::ReviewVerifyRerun.key(),
            1,
            "verify"
        )
        .await
        .unwrap()
        .charged
    );
    tx.commit().await.unwrap();
}
#[tokio::test]
async fn budget_verify_rerun_receipts_replay_and_new_reviews_get_fresh_windows() {
    let (db, task) = fixture().await;
    let workflow: WorkflowDefinition = serde_json::from_value(json!({"roles":[],"states":[{"name":"review","kind":"gate","column":"Review","display_name":"Review","hooks":{},"config":{"retry_budgets":{"review_verify_rerun":3}}}]})).unwrap();
    sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES('e','t','coder','completed','now','now')").execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,created_at,updated_at) VALUES('r1','t','e',1,'running','{}','now','now','now')").execute(db.pool()).await.unwrap();
    let config = json!({"retry_budgets":{"review_verify_rerun":1}});
    initialize_verify_budget(&db, &task, &workflow, "r1")
        .await
        .unwrap();
    assert!(grant_verify_rerun(&db, &task, &workflow, &config, "r1", 0)
        .await
        .unwrap());
    assert!(grant_verify_rerun(&db, &task, &workflow, &config, "r1", 0)
        .await
        .unwrap());
    assert_eq!(
        spent(db.pool(), &task.id, "review_verify_rerun")
            .await
            .unwrap(),
        1
    );
    initialize_verify_budget(&db, &task, &workflow, "r1")
        .await
        .unwrap();
    assert!(!grant_verify_rerun(&db, &task, &workflow, &config, "r1", 1)
        .await
        .unwrap());
    sqlx::query("UPDATE review SET status='cancelled' WHERE id='r1'")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,created_at,updated_at) VALUES('r2','t','e',2,'running','{}','now','now','now')").execute(db.pool()).await.unwrap();
    initialize_verify_budget(&db, &task, &workflow, "r2")
        .await
        .unwrap();
    assert_eq!(
        spent(db.pool(), &task.id, "review_verify_rerun")
            .await
            .unwrap(),
        0
    );
    assert!(grant_verify_rerun(&db, &task, &workflow, &config, "r2", 0)
        .await
        .unwrap());
    assert!(
        grant_verify_rerun(&db, &task, &workflow, &config, "r1", 0)
            .await
            .is_err(),
        "stale Review cannot restore its budget window"
    );
    assert_eq!(
        spent(db.pool(), &task.id, "review_verify_rerun")
            .await
            .unwrap(),
        1
    );
    let projected = projection(
        &task,
        &workflow,
        &HashMap::from([("review_verify_rerun".into(), 1)]),
        0,
    )
    .unwrap();
    assert_eq!(projected["review_verify_rerun"], 2);
}

#[tokio::test]
async fn budget_verify_rerun_charges_custom_review_state_and_fences_status() {
    let (db, _) = fixture().await;
    let workflow: WorkflowDefinition = serde_json::from_value(json!({"roles":[],"states":[
        {"name":"quality_check","kind":"gate","role":"reviewer","canonical_phase":"review","column":"Review","display_name":"Quality","hooks":{},"config":{},"gate_config":{"max_rejections":2}},
        {"name":"working","kind":"active","column":"Working","display_name":"Working","hooks":{},"config":{}}
    ]})).unwrap();
    sqlx::query("UPDATE project SET workflow_definition=? WHERE id='p'")
        .bind(serde_json::to_string(&workflow).unwrap())
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task SET status='quality_check' WHERE id='t'")
        .execute(db.pool())
        .await
        .unwrap();
    let task = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
    sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES('e','t','coder','completed','now','now')").execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,created_at,updated_at) VALUES('r','t','e',1,'running','{}','now','now','now')").execute(db.pool()).await.unwrap();
    let settings =
        json!({"retry_budgets":{"review_verify_rerun":3,"review_progress":4}}).to_string();
    let workflow = with_project_defaults(&workflow, &settings);
    let config = &workflow.states[0].config;
    initialize_verify_budget(&db, &task, &workflow, "r")
        .await
        .unwrap();
    assert!(grant_verify_rerun(&db, &task, &workflow, config, "r", 0)
        .await
        .unwrap());
    assert!(grant_verify_rerun(&db, &task, &workflow, config, "r", 0)
        .await
        .unwrap());
    let counts = load(db.pool(), &task.id).await.unwrap();
    assert_eq!(counts[Kind::ReviewVerifyRerun.key()], 1);
    let projected = projection(&task, &workflow, &counts, 0).unwrap();
    assert_eq!(projected["review_verify_rerun"], 2);
    assert_eq!(projected["review_progress"], 4);
    assert_eq!(projected["quality_check"], 2);
    assert_eq!(projected["review_gate"], 1);
    assert!(!gate_entry_exhausted(
        is_review_state(&workflow.states[0]),
        0,
        0
    ));
    assert!(gate_entry_exhausted(false, 0, 0));
    let gate = workflow.states[0].gate_config.as_ref();
    assert!(cancelled_review_entry_allows_retry(&db, &task, gate)
        .await
        .unwrap());

    let owner = api_types::Actor::user(api_types::UserActionSource::Api);
    let bridge = api_types::TransitionBridge::review_rerun(&owner);
    sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,created_at,bridge_kind,bridge_payload,status_epoch) VALUES('owner-entry','t','quality_check','quality_check','system','entry','2026-01-01T00:00:00Z',?,?,1)")
        .bind(bridge.bridge_kind.map(api_types::TransitionBridgeKind::as_str))
        .bind(bridge.bridge_payload.map(|payload| payload.to_string()))
        .execute(db.pool()).await.unwrap();
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    assert!(owner_requested_review_entry(&mut tx, &task.id, None, None)
        .await
        .unwrap());
    transition(
        &mut tx,
        &task.id,
        &task.status,
        false,
        &api_types::TransitionBridge::default(),
        true,
        "owner-rejection",
        None,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(review_spent(&db).await, (0, 0));

    sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,created_at,status_epoch) VALUES('automatic-entry','t','working','quality_check','system','entry','2026-01-01T00:00:01Z',2)")
        .execute(db.pool()).await.unwrap();
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    assert!(!owner_requested_review_entry(&mut tx, &task.id, None, None)
        .await
        .unwrap());
    transition(
        &mut tx,
        &task.id,
        &task.status,
        false,
        &api_types::TransitionBridge::default(),
        true,
        "automatic-rejection",
        None,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(review_spent(&db).await, (1, 1));
    assert!(!cancelled_review_entry_allows_retry(&db, &task, gate)
        .await
        .unwrap());
    assert_eq!(gate_budget_key(&workflow.states[0]), Kind::Review.key());

    sqlx::query("UPDATE task SET status='working',version=version+1 WHERE id='t'")
        .execute(db.pool())
        .await
        .unwrap();
    assert!(matches!(
        grant_verify_rerun(&db, &task, &workflow, config, "r", 1).await,
        Err(DbError::VersionConflict)
    ));
    let task = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
    assert!(matches!(
        initialize_verify_budget(&db, &task, &workflow, "r").await,
        Err(DbError::InvalidTransition)
    ));
    assert_eq!(
        spent(db.pool(), &task.id, "review_verify_rerun")
            .await
            .unwrap(),
        1
    );
}

/// A reviewer-role gate outside the review phase (a design check) runs no
/// Review, so nothing would ever park it at verdict time: it keeps its own
/// gate budget, entry cap and configuration. The built-in name keeps the
/// review rules whatever its kind.
#[tokio::test]
async fn budget_reviewer_gate_outside_the_review_phase_keeps_its_gate_budget() {
    let (db, _) = fixture().await;
    let workflow: WorkflowDefinition = serde_json::from_value(json!({"roles":[],"states":[
        {"name":"design_review","kind":"gate","role":"reviewer","canonical_phase":"working","column":"In Progress","display_name":"Design","hooks":{},"config":{},"gate_config":{"max_rejections":2}},
        {"name":"working","kind":"active","column":"Working","display_name":"Working","hooks":{},"config":{}}
    ]})).unwrap();
    sqlx::query("UPDATE project SET workflow_definition=? WHERE id='p'")
        .bind(serde_json::to_string(&workflow).unwrap())
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task SET status='design_review' WHERE id='t'")
        .execute(db.pool())
        .await
        .unwrap();
    let task = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
    let gate = &workflow.states[0];
    assert!(!is_review_state(gate));
    assert_eq!(gate_budget_key(gate), "gate:design_review");
    assert!(gate_entry_exhausted(is_review_state(gate), 2, 2));
    let settings = json!({
        "retry_budgets":{"review_progress":4},
        "default_review_config":{"retry_budgets":{"review":7}}
    })
    .to_string();
    assert_eq!(state_config(gate, &settings), json!({}));

    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    transition(
        &mut tx,
        &task.id,
        &task.status,
        false,
        &api_types::TransitionBridge::default(),
        true,
        "design-rejection",
        None,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(review_spent(&db).await, (0, 0));
    let counts = load(db.pool(), &task.id).await.unwrap();
    assert_eq!(counts["gate:design_review"], 1);
    let projected = projection(&task, &workflow, &counts, 0).unwrap();
    assert_eq!(projected["design_review"], 1);
    assert!(!projected.contains_key("review_gate"));

    let mut review = gate.clone();
    review.name = "review".into();
    review.kind = StateKind::Active;
    assert!(is_review_state(&review));
    assert_eq!(
        state_config(&review, &settings)["retry_budgets"]["review"],
        7
    );
}

// D5 rule 1 is Forge's own rule: a reviewer finding on its third check
// parks even when a required conformance check also failed (which
// replaces the reported reason). Only owner routing (`fixable_by`) and the
// self-reported `repeat` require the reviewer's verdict to be the cause.
#[test]
fn derived_repeat_parks_when_a_required_check_also_failed() {
    let row = |attempt: i64, details: Value| Review {
        id: format!("review-{attempt}"),
        task_id: "t".into(),
        execution_id: "e".into(),
        reviewer_execution_id: None,
        auditor_execution_id: None,
        attempt_number: attempt,
        status: ReviewStatus::Failed,
        step_results_json: details.to_string(),
        started_at: format!("2026-10-15T00:00:{attempt:02}Z"),
        finished_at: None,
        created_at: format!("2026-10-15T00:00:{attempt:02}Z"),
        updated_at: "now".into(),
    };
    let assessment = |findings: Value| json!({"result":"fail","reason":"problems","findings":findings,"repeat":true});
    let window = vec![
        row(
            1,
            json!({"conformance":{"status":"failed","checks":[],"reason":"problems",
            "assessment":assessment(json!([{"summary":"stuck"},{"summary":"other"}]))}}),
        ),
        row(
            2,
            json!({"conformance":{"status":"failed","checks":[],"reason":"problems",
            "assessment":assessment(json!([{"id":"F1","summary":"stuck"},{"id":"F2","summary":"other"}]))}}),
        ),
    ];
    let current = row(3, json!({}));
    for owner in [false, true] {
        let mut verdict = assessment(json!([{"id":"F1","summary":"stuck"}]));
        if owner {
            verdict["fixable_by"] = json!("owner");
        }
        let conformance: api_types::ReviewConformance = serde_json::from_value(json!({
            "status":"failed","reason":"required conformance check failed",
            "checks":[{"check_id":"lint","command":"lint","exit_code":1,"output":"bad"}],
            "assessment":verdict,
        }))
        .unwrap();
        assert_eq!(
            owner_review_failure_message(&conformance, &current, &window, &window, true).as_deref(),
            Some("repeated finding: F1 stuck"),
            "owner={owner}"
        );
        // Derived rules off: the reviewer did not cause this failure, so
        // neither its owner routing nor its self-reported repeat applies.
        assert_eq!(
            owner_review_failure_message(&conformance, &current, &window, &window, false),
            None
        );
    }
}
#[tokio::test]
async fn budget_coordination_root_resolves_the_workflow_planning_role() {
    let (db, task) = fixture().await;
    let workflow: WorkflowDefinition = serde_json::from_value(json!({"states":[
        {"name":"todo","kind":"initial","column":"Todo","display_name":"Todo","hooks":{},"config":{},"triggers":{"accept":{"to":"design"}}},
        {"name":"design","kind":"gate","column":"Planning","display_name":"Planning","hooks":{},"config":{},"role":"architect","triggers":{"accept":{"to":"working"}}},
        {"name":"working","kind":"active","column":"Working","display_name":"Working","hooks":{},"config":{},"role":"builder"},
        {"name":"review","kind":"gate","column":"Review","display_name":"Review","hooks":{},"config":{},"role":"reviewer","canonical_phase":"review"}
    ], "roles":[]})).unwrap();
    assert_eq!(
        workflow
            .coordination_planning_state()
            .unwrap()
            .role
            .as_deref(),
        Some("architect")
    );
    sqlx::query("UPDATE project SET workflow_definition=? WHERE id='p'")
        .bind(serde_json::to_string(&workflow).unwrap())
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,parent_task_id,created_at,updated_at) VALUES('child','p','Child','done','t','now','now')").execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO task_role_assignment(id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at) VALUES('lead','t','architect','agent','agent','now','now')").execute(db.pool()).await.unwrap();
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    assert!(coordination_root_in_tx(&mut tx, &task).await.unwrap());
    tx.rollback().await.unwrap();
    sqlx::query("UPDATE task_role_assignment SET role_name='planner' WHERE id='lead'")
        .execute(db.pool())
        .await
        .unwrap();
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    assert!(
        !coordination_root_in_tx(&mut tx, &task).await.unwrap(),
        "a planner outside the planning gate is not a lead"
    );
}
#[test]
fn budget_anchoring_nonblocking_park_uses_triaged_conformance_outcome() {
    let conformance: api_types::ReviewConformance = serde_json::from_value(json!({
        "status":"failed", "checks":[{"check_id":"ci:0","command":"tests","exit_code":1,"output":"flake", "triage":{"mode":"full","base_status":"available","blocking":false,"class":"flaky","failures":[]}}],
        "reason":"separate scope", "assessment":{"result":"fail","reason":"separate scope","findings":[{"summary":"later work","kind":"out_of_scope"}]}
    })).unwrap();
    let current = Review {
        id: "r".into(),
        task_id: "t".into(),
        execution_id: "e".into(),
        reviewer_execution_id: None,
        auditor_execution_id: None,
        attempt_number: 1,
        status: ReviewStatus::Failed,
        step_results_json: json!({"conformance":conformance}).to_string(),
        started_at: "now".into(),
        finished_at: None,
        created_at: "now".into(),
        updated_at: "now".into(),
    };
    for derived_rules in [true, false] {
        assert_eq!(
            owner_review_failure_message(&conformance, &current, &[], &[], derived_rules)
                .as_deref(),
            Some("only non-blocking findings remain")
        );
    }
}
#[tokio::test]
async fn budget_coordination_root_legacy_default_agrees_with_services_planner() {
    let (db, task) = fixture().await;
    sqlx::query("INSERT INTO task(id,project_id,title,status,parent_task_id,created_at,updated_at) VALUES('child','p','Child','done','t','now','now')").execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO task_role_assignment(id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at) VALUES('lead','t','planner','agent','agent','now','now')").execute(db.pool()).await.unwrap();
    for raw in ["{}", "", "not-json"] {
        sqlx::query("UPDATE project SET workflow_definition=? WHERE id='p'")
            .bind(raw)
            .execute(db.pool())
            .await
            .unwrap();
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        assert!(
            coordination_root_in_tx(&mut tx, &task).await.unwrap(),
            "services resolves {raw:?} to its default planner"
        );
        tx.rollback().await.unwrap();
    }
}
