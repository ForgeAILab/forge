use super::*;
use crate::task_condition::channels as channel;
use crate::{EnqueueTaskStep, TaskRepo, TaskStepRepo};
use serde_json::{json, Value};

#[test]
fn lane_b_barrier_constructor_matches_main_corpus() {
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
async fn lane_b_exhausted_review_entry_matches_main_after_next_write() {
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
        .claim_step("lane-b", Some("t"), "2099-01-01T00:00:00+00:00")
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

#[tokio::test]
async fn lane_b_sql_appends_keep_envelopes_and_machine_byte_fences() {
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
        .claim_step("lane-b", Some("t"), "2099-01-01T00:00:00+00:00")
        .await
        .unwrap()
        .unwrap();
    crate::task_writer::in_task_step(step, async {
        let machine = channel::ConditionMachine::Daemon {
            daemon_id: Some("daemon".into()), runtime_id: Some("runtime".into()),
        };
        let machine_json = serde_json::to_string(&machine).unwrap();
        assert_eq!(machine_json, r#"{"daemon_id":"daemon","owner_kind":"daemon","runtime_id":"runtime"}"#);
        let wait = channel::EnvironmentWait {
            checks: vec!["check".into()], kind: Some(channel::EnvironmentWaitKind::EnvironmentProbePending),
            last_error: None, machine: machine.clone(),
        };
        let metadata = format!(r#"{{"deferred_dispatch":{{"not_before":"later","target_state":"review"}},"environment_wait":{},"caller_extension":[1,null]}}"#,serde_json::to_string(&wait).unwrap());
        sqlx::query("UPDATE task SET metadata_json=? WHERE id='t'").bind(&metadata).execute(db.pool()).await.unwrap();
        assert_eq!(db.repository_provision_environment_wait_tasks("p",&machine_json).await.unwrap(), vec![("t".into(),task.version)]);
        let mut tx = db.pool().begin().await.unwrap();
        db.record_environment_probe_error_for_task_waits_in_tx(&mut tx,"probe failed","p",machine_json.clone()).await.unwrap();
        tx.commit().await.unwrap();
        let row = TaskRepo::get_by_id(&db,"t",false).await.unwrap().unwrap();
        let raw = row.metadata_json.unwrap();
        let expected = format!(r#"{{"deferred_dispatch":{{"not_before":"later","target_state":"review","reason":"environment_probe_pending; last failure: probe failed"}},"environment_wait":{{"checks":["check"],"kind":"environment_probe_pending","machine":{},"last_error":"probe failed"}},"caller_extension":[1,null]}}"#,machine_json);
        assert_eq!(raw,expected);
        let value:Value = serde_json::from_str(&raw).unwrap();
        let typed:channel::EnvironmentWait=serde_json::from_value(value["environment_wait"].clone()).unwrap();
        assert_eq!(typed, channel::EnvironmentWait { last_error:Some("probe failed".into()), ..wait });
        let typed:channel::DeferredDispatch=serde_json::from_value(value["deferred_dispatch"].clone()).unwrap();
        assert_eq!(typed,channel::DeferredDispatch {kind:None,not_before:"later".into(),reason:"environment_probe_pending; last failure: probe failed".into(),target_state:"review".into()});
        // A provision refresh also appends reason on a historical marker that lacked it.
        sqlx::query("UPDATE task SET metadata_json=? WHERE id='t'").bind(&metadata).execute(db.pool()).await.unwrap();
        let mut tx=db.pool().begin().await.unwrap();
        let applied = db.record_repository_provision_task_wait_in_tx(&mut tx,crate::RepositoryProvisionTaskWaitInput {
            task_id:"t",reason:"provision failed",version:task.version,machine_json:&machine_json,project_id:"p",
            daemon_id:&Some("daemon".into()),runtime_id:&Some("runtime".into()),readiness_version:-1,
        }).await.unwrap();
        assert_eq!(applied.require_applied().unwrap(), 1);
        tx.commit().await.unwrap();
        let row=TaskRepo::get_by_id(&db,"t",false).await.unwrap().unwrap();
        assert_eq!(row.metadata_json.as_deref(),Some(metadata.replacen(r#""target_state":"review"}"#,r#""target_state":"review","reason":"provision failed"}"#,1).as_str()));
        // A review-CI barrier initially has no started_at; SQL appends it last.
        let initial = r#"{"blocking_reason":"ci unavailable","interrupted_at":"at","state":"review","status":"blocked","updated_at":"at"}"#;
        sqlx::query("UPDATE task SET entry_barrier_json=? WHERE id='t'").bind(initial).execute(db.pool()).await.unwrap();
        let mut tx=db.pool().begin().await.unwrap();
        db.restart_owner_reconnect_review_barrier_in_tx(&mut tx,"window","t").await.unwrap();
        tx.commit().await.unwrap();
        let row=TaskRepo::get_by_id(&db,"t",false).await.unwrap().unwrap();
        let raw=row.entry_barrier_json.unwrap();
        assert_eq!(raw,r#"{"blocking_reason":"ci unavailable","interrupted_at":"at","state":"review","status":"blocked","updated_at":"at","started_at":"window"}"#);
        let typed:channel::EntryBarrier=serde_json::from_str(&raw).unwrap();
        assert_eq!(typed.started_at,Some(Some("window".into())));
        assert_ne!(serde_json::to_string(&typed).unwrap(),raw,"SQL append must retain its original member order");
    }).await;
}
