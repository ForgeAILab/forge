//! Refactor 3.2 stage B shadow-observation cost measurement (not a
//! correctness test). File-backed SQLite, the result transactions the merge
//! path already runs, with and without an observation to record.
//!
//! Uses only API that exists on the first-pass commit, so the same file runs
//! on both sides of the audit:
//! `cargo test -p db --test integration_shadow_cost -- --ignored --nocapture`
use db::{EnqueueTaskStep, SqliteDb, TaskStep, TaskStepRepo};
use serde_json::json;
use std::time::{Duration, Instant};

const T: &str = "2026-10-07T00:00:00Z";

async fn step_for(db: &SqliteDb, task: &str, lane: &str) -> TaskStep {
    let id = db::new_uuid_v4();
    db.enqueue_step(&EnqueueTaskStep {
        id: id.clone(),
        task_id: task.into(),
        kind: "hooks".into(),
        payload_json: "{}".into(),
        causation_step_id: None,
        causation_key: id.clone(),
        chain_id: id.clone(),
        chain_position: 1,
        expected_status: "merging".into(),
        expected_version: 1,
        expected_epoch: None,
        lane: lane.into(),
        available_at: db::now_rfc3339(),
    })
    .await
    .unwrap();
    db.claim_step("cost", Some(task), &db::task_writer::lease_deadline())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
#[ignore = "measurement"]
async fn measure_shadow_observation_cost() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shadow-cost.db");
    let pool = db::create_sqlite_pool(&format!("sqlite:{}", path.display()))
        .await
        .unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    let checkout = dir.path().join("checkout").to_str().unwrap().to_owned();
    sqlx::query("INSERT INTO project(id,name,settings,workflow_definition,created_at,updated_at) VALUES('p','p','{}','{}',?,?)").bind(T).bind(T).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO repo(id,project_id,name,local_path,default_branch,created_at,updated_at) VALUES('r','p','r',?,'main',?,?)").bind(&checkout).bind(T).bind(T).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) VALUES('l','r','server',?,'primary_checkout',1,'ready',?,?)").bind(&checkout).bind(T).bind(T).execute(db.pool()).await.unwrap();
    for task in ["plain", "merge"] {
        sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES(?,'p',?,'merging',?,?)").bind(task).bind(task).bind(T).bind(T).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES(?,?,'r',?,'task/branch','ready',?,?)").bind(format!("w-{task}")).bind(task).bind(&checkout).bind(T).bind(T).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO execution(id,task_id,role,status,workspace_id,created_at,updated_at) VALUES(?,?,'executor','completed',?,?,?)").bind(format!("e-{task}")).bind(task).bind(format!("w-{task}")).bind(T).bind(T).execute(db.pool()).await.unwrap();
    }
    let n = 1000u32;
    let per = |d: Duration| d / n;
    let plain = step_for(&db, "plain", "long").await;
    let merge = step_for(&db, "merge", "long").await;
    for step in [&plain, &merge] {
        sqlx::query(
            "INSERT INTO task_hook_checkpoint(step_id,hook_index,started_at) VALUES(?,0,?)",
        )
        .bind(&step.id)
        .bind(T)
        .execute(db.pool())
        .await
        .unwrap();
    }
    let rebased = json!({"kind":"rebased"}).to_string();
    let intent = json!({"execution_id":"e-merge","workspace_id":"w-merge","candidate_sha":"candidate","target_branch":"main"}).to_string();
    for round in 0..2 {
        // A. The result transaction with nothing to observe (unobserved key).
        let t = Instant::now();
        for _ in 0..n {
            db.record_hook_effect(&plain, 0, "unobserved", &rebased)
                .await
                .unwrap();
        }
        let base = per(t.elapsed());
        // B. An observed key on a Task with no current attempt.
        let t = Instant::now();
        for _ in 0..n {
            db.record_hook_effect(&plain, 0, "rebase_outcome", &rebased)
                .await
                .unwrap();
        }
        let no_attempt = per(t.elapsed());
        // C. Admission: the first merge intent of a merge entry.
        let mut admission = Duration::ZERO;
        for _ in 0..n {
            sqlx::raw_sql("UPDATE integration_queue SET head_attempt_id=NULL; DELETE FROM integration_attempt; DELETE FROM integration_queue;")
                .execute(db.pool())
                .await
                .unwrap();
            let t = Instant::now();
            db.record_hook_effect(&merge, 0, "merge_intent", &intent)
                .await
                .unwrap();
            admission += t.elapsed();
        }
        let admission = per(admission);
        // D. A new observation on the current attempt, 1,000 times: a Task
        // that keeps looping through rebase.
        let t = Instant::now();
        let mut first = Duration::ZERO;
        for i in 0..n {
            db.record_hook_effect(&merge, i64::from(i) + 1, "rebase_outcome", &rebased)
                .await
                .unwrap();
            if i == 29 {
                first = t.elapsed() / 30;
            }
        }
        let appended = per(t.elapsed());
        // E. Replay of an identity already recorded.
        let t = Instant::now();
        for _ in 0..n {
            db.record_hook_effect(&merge, i64::from(n), "rebase_outcome", &rebased)
                .await
                .unwrap();
        }
        let replay = per(t.elapsed());
        let bytes: i64 = sqlx::query_scalar(
            "SELECT length(CAST(observations_json AS BLOB)) FROM integration_attempt",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        let pct = |d: Duration| (d.as_secs_f64() / base.as_secs_f64() - 1.0) * 100.0;
        println!("COST hook[{round}] base={base:?} no_attempt={no_attempt:?} ({:+.0}%) admission={admission:?} ({:+.0}%) append_first30={first:?} ({:+.0}%) append_1000={appended:?} ({:+.0}%) replay={replay:?} ({:+.0}%) observations_bytes={bytes}", pct(no_attempt), pct(admission), pct(first), pct(appended), pct(replay));
    }
    // F. Step settlement: every step of every Task passes the terminal site.
    for (label, task) in [("no_attempt", "plain"), ("current_attempt", "merge")] {
        for step in [&plain, &merge] {
            sqlx::query("UPDATE task_step SET status='done',completed_at='x' WHERE id=?")
                .bind(&step.id)
                .execute(db.pool())
                .await
                .unwrap();
            db.release_step(&step.id, "cost").await.unwrap();
        }
        let mut finish = Duration::ZERO;
        for _ in 0..n {
            let step = step_for(&db, task, "fast").await;
            let t = Instant::now();
            let mut tx = db::begin_immediate(db.pool()).await.unwrap();
            db.finish_step_in_tx(&mut tx, &step, "done", None)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            finish += t.elapsed();
            db.release_step(&step.id, "cost").await.unwrap();
        }
        println!("COST finish_step[{label}]={:?}", per(finish));
    }
}
