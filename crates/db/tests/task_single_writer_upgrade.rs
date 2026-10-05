//! Remote-cancel cleanup records vs Project deletion, and the
//! single-writer migration on a populated next/v0.14 (3.5 + 3.6) database.
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, run_migrations_from, ProjectRepo,
    SqliteDb, TaskMutation,
};
use std::path::PathBuf;

/// id, status, claimed_by, lease_until, attempts, expected_epoch, lane,
/// causation_step_id.
type StepRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    i64,
    i64,
    String,
    Option<String>,
);

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("forge-single-writer")
        .join(format!("{name}-{}", new_uuid_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn seeded(marker: bool) -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    let now = now_rfc3339();
    let root = scratch("ws");
    sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES('p','p',?,?)")
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t','p','t','cancelled',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO repo(id,project_id,name,default_branch,created_at,updated_at) VALUES('r','p','r','main',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES('w','t','r',?,'task/t','ready',?,?)").bind(root.to_string_lossy().as_ref()).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,status,created_at,updated_at) VALUES('l','r','server',?,'primary_checkout','ready',?,?)").bind(root.to_string_lossy().as_ref()).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO workspace_placement(id,workspace_id,task_id,owner_kind,repo_location_id,workspace_handle,state,selected_by,selection_reason,created_at,updated_at) VALUES('placement','w','t','server','l','handle','ready','backfill','{}',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    let step_id = db
        .enqueue_task_mutation(
            "t",
            TaskMutation::TaskSetEntryBarrier {
                id: "t".into(),
                expected_version: 1,
                entry_barrier_json: None,
                updated_at: now.clone(),
            },
        )
        .await
        .unwrap();
    sqlx::query("INSERT INTO task_remote_operation(operation_id,step_id,workspace_id,placement_id,daemon_id,runtime_id,generation,expected_epoch,state,created_at) VALUES('operation',?,'w','placement','owner','runtime',1,0,?,?)")
        .bind(&step_id).bind(if marker { "running" } else { "finished" }).bind(&now).execute(db.pool()).await.unwrap();
    if marker {
        let operation = db
            .running_remote_task_operations(&step_id)
            .await
            .unwrap()
            .remove(0);
        db.mark_pending_remote_cancel(&operation).await.unwrap();
    }
    // The step settles normally; only the remote-operation rows remain.
    sqlx::query("UPDATE task_step SET status='superseded',completed_at=? WHERE id=?")
        .bind(&now)
        .bind(&step_id)
        .execute(db.pool())
        .await
        .unwrap();
    db
}

/// A daemon that never reconnects leaves a pending_remote_cancel marker.
/// The owner must still be able to delete the Project.
#[tokio::test]
async fn project_delete_with_unacknowledged_remote_cancel() {
    let db = seeded(true).await;
    let result = ProjectRepo::delete(&db, "p").await;
    assert!(
        result.is_ok(),
        "Project delete with a pending_remote_cancel marker failed: {result:?}"
    );
}

/// Even an acknowledged/finished remote operation row restricts the Project
/// cascade through workspace/placement ON DELETE RESTRICT.
#[tokio::test]
async fn project_delete_with_finished_remote_operation() {
    let db = seeded(false).await;
    let result = ProjectRepo::delete(&db, "p").await;
    assert!(
        result.is_ok(),
        "Project delete with a finished task_remote_operation row failed: {result:?}"
    );
}

/// Upgrade a populated next/v0.14 (39733dce) database: every migration up to
/// and including 3.6's V202610050233, real rows, then the 2.3c migration.
#[tokio::test]
async fn single_writer_upgrade_on_populated_next_database() {
    let full = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"));
    let base = scratch("base-migrations");
    for entry in std::fs::read_dir(&full).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        if name.as_str() < "V202610051045" {
            std::fs::copy(entry.path(), base.join(&name)).unwrap();
        }
    }
    let db_dir = scratch("upgrade-db");
    let pool = create_sqlite_pool(&format!(
        "sqlite://{}",
        db_dir.join("forge.sqlite").display()
    ))
    .await
    .unwrap();
    run_migrations_from(&pool, &base).await.unwrap();
    let applied: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _migration ORDER BY version DESC LIMIT 3")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(applied, vec![202610050233, 202610042145, 202610040225]);

    let now = now_rfc3339();
    sqlx::raw_sql(&format!(
        "INSERT INTO project(id,name,created_at,updated_at) VALUES ('p','p','{now}','{now}');
         INSERT INTO task(id,project_id,title,status,created_at,updated_at,status_epoch) VALUES ('t','p','t','review','{now}','{now}',5);
         INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES ('u','p','u','todo','{now}','{now}');
         INSERT INTO task_step(id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,status,claimed_by,lease_until,available_at,attempts,created_at,updated_at,expected_epoch,lane)
           VALUES('hooks','t',1,'hooks','{{\"workflow_ref\":{{\"id\":\"wf\"}}}}','entry','chain',1,'review',7,'claimed','owner','2099-01-01T00:00:00Z','2000-01-01T00:00:00Z',2,'{now}','{now}',5,'long');
         INSERT INTO task_step(id,task_id,seq,kind,payload_json,causation_step_id,causation_key,chain_id,chain_position,expected_status,expected_version,status,available_at,created_at,updated_at,expected_epoch,lane)
           VALUES('cascade','t',2,'cascade','{{}}','hooks','next','chain',2,'review',7,'pending','2000-01-01T00:00:00Z','{now}','{now}',5,'fast');
         INSERT INTO task_step(id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,status,available_at,created_at,updated_at,completed_at,expected_epoch,lane)
           VALUES('old','u',1,'cascade','{{}}','old','old',1,'todo',1,'done','2000-01-01T00:00:00Z','{now}','{now}','{now}',0,'fast');
         INSERT INTO task_hook_checkpoint(step_id,hook_index,started_at,result_json,effects_json) VALUES('hooks',0,'{now}','{{\"Ok\":null}}','{{\"effect\":\"kept\"}}');
         INSERT INTO task_hook_checkpoint(step_id,hook_index,started_at,effects_json) VALUES('hooks',1,'{now}','{{}}');
         INSERT INTO task_hook_script(step_id,hook_index,script_index,started_at,result_json) VALUES('hooks',1,0,'{now}','{{\"status\":\"passed\"}}');"
    ))
    .execute(&pool)
    .await
    .unwrap();

    run_migrations(&pool).await.unwrap();

    let steps: Vec<StepRow> =
        sqlx::query_as("SELECT id,status,claimed_by,lease_until,attempts,expected_epoch,lane,causation_step_id FROM task_step ORDER BY task_id,seq")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(steps.len(), 3);
    assert_eq!(
        steps[0],
        (
            "hooks".into(),
            "claimed".into(),
            Some("owner".into()),
            Some("2099-01-01T00:00:00Z".into()),
            2,
            5,
            "long".into(),
            None
        )
    );
    assert_eq!(steps[1].7.as_deref(), Some("hooks"));
    assert_eq!(steps[2].1, "done");
    let checkpoints: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_hook_checkpoint WHERE step_id='hooks'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(checkpoints, 2);
    let effects: String = sqlx::query_scalar(
        "SELECT effects_json FROM task_hook_checkpoint WHERE step_id='hooks' AND hook_index=0",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(effects, "{\"effect\":\"kept\"}");
    let scripts: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_hook_script WHERE step_id='hooks'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(scripts, 1);
    let wf_ref: Option<String> =
        sqlx::query_scalar("SELECT workflow_ref_id FROM task_step WHERE id='hooks'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(wf_ref.as_deref(), Some("wf"));
    let fk: Vec<(String,)> = sqlx::query_as("SELECT \"table\" FROM pragma_foreign_key_check")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert!(
        fk.is_empty(),
        "foreign key violations after upgrade: {fk:?}"
    );
    let fk_on: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(fk_on, 1);
    let indexes: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND tbl_name='task_step' AND name LIKE 'task_step_%'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(indexes, 7);
    // Idempotent re-run.
    run_migrations(&pool).await.unwrap();
    // The upgraded step queue still deletes cleanly with its Task.
    sqlx::query("DELETE FROM task WHERE id='t'")
        .execute(&pool)
        .await
        .unwrap();
    let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_hook_script")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(left, 0);
}
