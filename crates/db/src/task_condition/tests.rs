use super::producer_tests::{enqueue, pending_cancel, set_parent, workspace};
use super::*;
use crate::{
    CreateProject, CreateTask, EnqueueTaskStep, ProjectRepo, TaskMetadataMutation, TaskRepo,
    TaskStepRepo, UpdateTask, UpdateTaskStatus,
};
use serde_json::{json, Value};
use sqlx::SqliteConnection;

/// Port of `audit_backfill_timing_5k` (measurement, not a correctness check).
/// `cargo test -p db --lib task_condition::tests::measure_ -- --ignored --nocapture`
#[tokio::test]
#[ignore = "measurement"]
async fn measure_backfill_5k() {
    let path = std::env::temp_dir().join(format!("condition-backfill-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let pool = crate::create_sqlite_pool(&format!("sqlite:{}", path.display()))
        .await
        .unwrap();
    crate::run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    ProjectRepo::create(
        &db,
        CreateProject {
            id: "p".into(),
            owner_id: None,
            name: "A".into(),
            primary_repo_id: None,
            updated_at: crate::now_rfc3339(),
            settings: "{}".into(),
            workflow_definition: "{}".into(),
            created_at: crate::now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let template = task(&db, "tmpl").await;
    clone_tasks(&db, &template.id, 5000, "r").await;
    unmigrate(&db).await;
    let meta = json!({"planning_completed_at":"2026-10-01T00:00:00Z","planning_execution_id":"e1","last_execution_failure_at":"2026-10-01T00:00:00Z","last_execution_failure_execution_id":"e2","custom":"z".repeat(1500),"dispatch_disposition":{"capability":"project_capacity","safe_message":"limit"}}).to_string();
    let ann =
        json!({"type":"ci_failed","message":"m".repeat(800),"blocked_at":"2026-10-01T00:00:00Z"})
            .to_string();
    sqlx::query("UPDATE task SET metadata_json=?")
        .bind(&meta)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task SET error_annotation=?, blocked_json=? WHERE (rowid % 5)=0")
        .bind(&ann)
        .bind(json!({"kind":"ci_failed","blocked_by":"system"}).to_string())
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(db.pool())
        .await
        .unwrap();
    let size0 = std::fs::metadata(&path).unwrap().len();
    let started = std::time::Instant::now();
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    migrate(&mut tx).await;
    let in_tx = started.elapsed();
    tx.commit().await.unwrap();
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(db.pool())
        .await
        .unwrap();
    let size1 = std::fs::metadata(&path).unwrap().len();
    let avg: f64 = sqlx::query_scalar("SELECT avg(length(condition_json)) FROM task")
        .fetch_one(db.pool())
        .await
        .unwrap();
    println!("MEASURE 5k backfill in BEGIN IMMEDIATE: {in_tx:?}; file {size0} -> {size1} bytes; avg condition_json {avg:.0} bytes");
    assert_eq!(
        db.task_condition_violations().await.unwrap(),
        Vec::<String>::new()
    );
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

/// Port of `audit_size_scaling` (measurement): cost of the pure mapping as the
/// row's legacy values grow.
#[test]
#[ignore = "measurement"]
fn measure_mapping_size_scaling() {
    for size in [1_000usize, 10_000, 100_000, 1_000_000] {
        let legacy = ConditionInput {
            error_annotation: Some(json!({"type":"ci_failed","message":"m".repeat(size)}).to_string()),
            metadata_json: Some(json!({"custom":"y".repeat(size),"deferred_dispatch":{"not_before":"2026-10-05T12:00:00Z","target_state":"todo","reason":"\u{06A9}".repeat(size / 2)}}).to_string()),
            ..Default::default()
        };
        let started = std::time::Instant::now();
        for _ in 0..20 {
            std::hint::black_box(encode(&compose_condition(&legacy)));
        }
        println!(
            "MEASURE map+encode size={size}: {:?}",
            started.elapsed() / 20
        );
    }
}

mod evidence;

#[path = "tests/helpers.rs"]
mod helpers;
use helpers::*;
pub(super) use helpers::{claim, db, input, task};
#[path = "tests/mapping.rs"]
mod mapping;
#[path = "tests/migration.rs"]
mod migration;
#[path = "tests/producer_shapes.rs"]
mod producer_shapes;
#[path = "tests/writers.rs"]
mod writers;
