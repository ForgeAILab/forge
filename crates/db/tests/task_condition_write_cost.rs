//! Refactor 3.1 stage 2 write-cost measurement (not a correctness test).
//!
//! Uses only API that exists on the base commit, so the same file runs on
//! both sides of the change:
//! `cargo test -p db --test task_condition_write_cost -- --ignored --nocapture`
use db::{
    CreateProject, CreateTask, EnqueueTaskStep, ProjectRepo, SqliteDb, TaskMetadataMutation,
    TaskRepo, TaskStepRepo,
};
use serde_json::json;
use std::time::Instant;

#[tokio::test]
#[ignore = "measurement"]
async fn measure_task_write_cost() {
    let path = std::env::temp_dir().join(format!("condition-cost-{}.db", std::process::id()));
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
    let pool = db::create_sqlite_pool(&format!("sqlite:{}", path.display()))
        .await
        .unwrap();
    db::run_migrations(&pool).await.unwrap();
    // Diagnosis only: statements to run first, e.g. dropping one trigger or
    // index to see what a write path pays for it.
    if let Ok(setup) = std::env::var("COST_SETUP_SQL") {
        sqlx::raw_sql(&setup).execute(&pool).await.unwrap();
        println!("COST setup: {setup}");
    }
    let db = SqliteDb::new(pool);
    ProjectRepo::create(
        &db,
        CreateProject {
            id: "p".into(),
            owner_id: None,
            name: "Cost".into(),
            primary_repo_id: None,
            updated_at: db::now_rfc3339(),
            settings: "{}".into(),
            workflow_definition: "{}".into(),
            created_at: db::now_rfc3339(),
        },
    )
    .await
    .unwrap();
    for i in 0..200 {
        TaskRepo::create(
            &db,
            CreateTask {
                id: format!("b{i}"),
                project_id: "p".into(),
                parent_task_id: None,
                assignee_type: None,
                assignee_id: None,
                title: "t".into(),
                description: None,
                task_type: "task".into(),
                status: "todo".into(),
                is_automation: false,
                priority: 0,
                task_state_config: None,
                merge_config: None,
                subtask_order: None,
                plan: None,
                updated_at: db::now_rfc3339(),
                created_at: db::now_rfc3339(),
            },
        )
        .await
        .unwrap();
    }
    let n = 2000u32;
    // The audit's method: one cached raw statement on one connection.
    let mut c = db.pool().acquire().await.unwrap();
    for round in 0..3 {
        let t = Instant::now();
        for i in 0..n {
            sqlx::query("UPDATE task SET metadata_json=?, updated_at=? WHERE id=?").bind(json!({"custom":i,"last_execution_failure_at":"2026-10-01T00:00:00Z","dispatch_disposition":{"capability":"project_capacity"}}).to_string()).bind("u").bind(format!("b{}", i % 200)).execute(&mut *c).await.unwrap();
        }
        let meta = t.elapsed() / n;
        let t = Instant::now();
        for i in 0..n {
            sqlx::query(
                "UPDATE task SET error_annotation=?, blocked_json=?, version=version+1 WHERE id=?",
            )
            .bind(json!({"type":"ci_failed","n":i}).to_string())
            .bind(json!({"kind":"ci_failed","n":i}).to_string())
            .bind(format!("b{}", i % 200))
            .execute(&mut *c)
            .await
            .unwrap();
        }
        let ann = t.elapsed() / n;
        let t = Instant::now();
        for i in 0..n {
            sqlx::query("UPDATE task SET updated_at=?, version=version+1 WHERE id=?")
                .bind(i.to_string())
                .bind(format!("b{}", i % 200))
                .execute(&mut *c)
                .await
                .unwrap();
        }
        let plain = t.elapsed() / n;
        let t = Instant::now();
        for i in 0..200 {
            sqlx::query(&format!(
                "UPDATE task SET metadata_json=?, updated_at=? WHERE id=? /*{round}-{i}*/"
            ))
            .bind("{}")
            .bind("u")
            .bind("b1")
            .execute(&mut *c)
            .await
            .unwrap();
        }
        let cold = t.elapsed() / 200;
        println!("MEASURE raw[{round}] metadata-update={meta:?} annotation+blocked-update={ann:?} non-legacy-update={plain:?} uncached-prepare+metadata-update={cold:?}");
    }
    sqlx::query("UPDATE task SET metadata_json=NULL,error_annotation=NULL,blocked_json=NULL")
        .execute(&mut *c)
        .await
        .unwrap();
    drop(c);

    // The writer seams: the repository metadata writer and a SQL-computed
    // Task write, both under the Task's claimed step.
    let task = TaskRepo::get_by_id(&db, "b0", false)
        .await
        .unwrap()
        .unwrap();
    let step_id = db::new_uuid_v4();
    db.enqueue_step(&EnqueueTaskStep {
        id: step_id.clone(),
        task_id: "b0".into(),
        kind: "command".into(),
        payload_json: "{}".into(),
        causation_step_id: None,
        causation_key: step_id.clone(),
        chain_id: step_id,
        chain_position: 1,
        expected_status: task.status,
        expected_version: task.version,
        expected_epoch: None,
        lane: "fast".into(),
        available_at: db::now_rfc3339(),
    })
    .await
    .unwrap();
    let step = db
        .claim_step("cost", Some("b0"), &db::task_writer::lease_deadline())
        .await
        .unwrap()
        .unwrap();
    db::task_writer::in_task_step(step, async {
        for round in 0..3 {
            let t = Instant::now();
            for i in 0..n {
                TaskRepo::mutate_metadata(&db, "b0", None, vec![TaskMetadataMutation::Set { key: "custom".into(), value: json!({"sequence":i,"note":"Budget awaiting_human queued_recovery"}) }], "u").await.unwrap();
            }
            let unrelated = t.elapsed() / n;
            let t = Instant::now();
            for i in 0..n { baseline_da4_metadata(&db, "b0", i).await; }
            let baseline = t.elapsed() / n;
            println!("COST ordinary[{round}] da4-baseline-ns={} stage2-ns={} ratio={:.4}", baseline.as_nanos(), unrelated.as_nanos(), unrelated.as_secs_f64()/baseline.as_secs_f64());
            let t = Instant::now();
            for i in 0..n {
                TaskRepo::mutate_metadata(&db, "b0", None, vec![TaskMetadataMutation::Set { key: "owner_wait".into(), value: json!({"daemon_id": i}) }], "u").await.unwrap();
            }
            let condition = t.elapsed() / n;
            let t = Instant::now();
            for i in 0..n {
                db::task_writer::TaskQuery::new(&db, "b0", "UPDATE task SET metadata_json=json_set(COALESCE(metadata_json,'{}'),'$.deferred_dispatch.reason',?) WHERE id=?").bind(i.to_string()).bind("b0").execute(db.pool()).await.unwrap();
            }
            let sql_computed = t.elapsed() / n;
            let t = Instant::now();
            for i in 0..n {
                db::task_writer::TaskQuery::new(&db, "b0", "UPDATE task SET updated_at=? WHERE id=?").bind(i.to_string()).bind("b0").execute(db.pool()).await.unwrap();
            }
            let non_legacy = t.elapsed() / n;
            println!("MEASURE seam[{round}] mutate_metadata(unrelated key)={unrelated:?} mutate_metadata(condition key)={condition:?} TaskQuery(sql-computed metadata)={sql_computed:?} TaskQuery(non-legacy)={non_legacy:?}");
        }
    })
    .await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

/// Exact ordinary Set-key path from da4c45fd::mutate_metadata_with_change,
/// exercised in the same migrated file/pool/lease and interleaved with modern
/// calls. No repository clone, alternate build or altered production schema.
async fn baseline_da4_metadata(db: &SqliteDb, id: &str, value: u32) {
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    db.fence_current_step_in_tx(&mut tx).await.unwrap();
    let task = TaskRepo::get_by_id_in_tx(db, &mut tx, id, false)
        .await
        .unwrap()
        .unwrap();
    let mut metadata = db::TaskMetadata::parse(task.metadata_json.as_deref()).unwrap();
    let new = json!({"sequence":value,"note":"Budget awaiting_human queued_recovery"});
    if metadata.extra.get("custom") == Some(&new) {
        tx.commit().await.unwrap();
        return;
    }
    metadata.extra.insert("custom".into(), new);
    sqlx::query("UPDATE task SET metadata_json=?,updated_at=? WHERE id=? AND deleted_at IS NULL")
        .bind(metadata.to_json())
        .bind("u")
        .bind(id)
        .execute(&mut *tx)
        .await
        .unwrap();
    // da4's reply adapter is a no-op for this command-kind lease.
    tx.commit().await.unwrap();
}
