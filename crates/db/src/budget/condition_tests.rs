use super::*;
use crate::{EnqueueTaskStep, TaskRepo, TaskStepRepo};
use serde_json::{json, Value};

#[test]
fn barrier_constructor_matches_main_corpus() {
    let _clock = crate::test_clock::freeze("2026-10-25T00:00:00.123456+00:00".parse().unwrap());
    let _ = crate::now_rfc3339(); // Main's corpus called the merge-budget writer first.
    let raw = exhausted_review_barrier("review", "2026-10-25T00:00:00.000000+00:00");
    let sample = include_str!("../task_condition/channels/fixtures/main-13bc2ae1.jsonl")
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|sample| sample["name"] == "budget-barrier")
        .unwrap();
    assert_eq!(raw, sample["raw"].as_str().unwrap());
}

async fn snapshot(db: &SqliteDb, phase: &str) {
    let row = TaskRepo::get_by_id(db, "t", false).await.unwrap().unwrap();
    let condition: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id='t'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let condition = crate::task_condition::test_support::without_channel_digest(&condition);
    let actual = json!({"case":format!("review-entry-budget/{phase}"),"fields":{
        "error_annotation":row.error_annotation,"blocked_json":row.blocked_json,"failed_json":row.failed_json,
        "entry_barrier_json":row.entry_barrier_json,"metadata_json":row.metadata_json,"condition_json":condition,
        "public":row.condition.public(),"read":row.condition.read(),"failure_kind":row.condition.read().failure_kind,
        "operations_text":row.condition.read().operator_reason,"attention_digest":crate::CreateDomainEvent::task_interruption_changed(&row).dedupe_key,
    }});
    let expected = include_str!("fixtures/main-94609411.jsonl")
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|value| value["case"] == actual["case"])
        .expect("archived main budget case");
    assert_eq!(actual, expected, "budget stored/projection parity: {phase}");
}

#[tokio::test]
async fn exhausted_review_entry_matches_main_after_next_write() {
    let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
    crate::run_migrations(&pool).await.unwrap();
    let _clock = crate::test_clock::freeze("2026-10-25T00:00:00.123456+00:00".parse().unwrap());
    sqlx::query(
        "INSERT INTO project(id,name,created_at,updated_at) VALUES('p','Project','now','now')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t','p','Task','review','now','now')").execute(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    let task = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
    db.enqueue_step(&EnqueueTaskStep {
        id: "step".into(),
        task_id: task.id.clone(),
        kind: "command".into(),
        payload_json: "{}".into(),
        causation_step_id: None,
        causation_key: "step".into(),
        chain_id: "step".into(),
        chain_position: 1,
        expected_status: task.status.clone(),
        expected_version: task.version,
        expected_epoch: None,
        lane: "fast".into(),
        available_at: crate::now_rfc3339(),
    })
    .await
    .unwrap();
    let step = db
        .claim_step("budget-parity", Some("t"), "2099-01-01T00:00:00+00:00")
        .await
        .unwrap()
        .unwrap();
    crate::task_writer::in_task_step(step, async {
        let (_, allowed) = failed_review_entry(&db, &task, 0, "hook", "2026-10-25T00:00:00Z")
            .await
            .unwrap();
        assert!(!allowed);
        snapshot(&db, "first").await;
        TaskRepo::mutate_metadata(
            &db,
            "t",
            None,
            vec![crate::TaskMetadataMutation::Set {
                key: "awaiting_human".into(),
                value: json!(false),
            }],
            &crate::now_rfc3339(),
        )
        .await
        .unwrap();
        snapshot(&db, "following-unconverted-write").await;
    })
    .await;
}
