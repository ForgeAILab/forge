//! Refactor 3.1 stage 2 write-cost measurement of the condition producers
//! (not a correctness test): a status transition, a budget charge, an
//! execution admission and an entry hooks step, for a root and for a child.
//!
//! Uses only API present on d6445863, so the same file runs on both sides:
//!
//! ```text
//! COST_WORKFLOW="$(cat workflow.json)" cargo test -p db --release \
//!   --test task_condition_producer_cost -- --ignored --nocapture
//! ```
//!
//! `COST_WORKFLOW` is the Project's `workflow_definition`; API-created
//! Projects store the full definition (about 16 KB), the default is `{}`.
//! Tasks carry 40 logged transitions and 10 settled executions, and each
//! execution round adds 200 more to the Task's history.
use db::{
    CreateExecution, CreateProject, CreateTask, EnqueueTaskStep, ExecutionRepo, ExecutionStatus,
    ProjectRepo, SqliteDb, TaskRepo, TaskStepRepo, UpdateTaskStatus,
};
use std::time::Instant;

async fn mk(db: &SqliteDb, id: &str, parent: Option<&str>) {
    TaskRepo::create(
        db,
        CreateTask {
            id: id.into(),
            project_id: "p".into(),
            parent_task_id: parent.map(Into::into),
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
            subtask_order: parent.map(|_| 1),
            plan: None,
            updated_at: db::now_rfc3339(),
            created_at: db::now_rfc3339(),
        },
    )
    .await
    .unwrap();
    // A mid-life Task: 40 logged transitions and 10 settled executions.
    for i in 0..40 {
        sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,created_at,status_epoch) VALUES(?,?,'todo','in_progress','system','h',?,?)")
            .bind(format!("{id}-l{i}")).bind(id).bind(format!("2026-01-01T00:00:{i:02}Z")).bind(-(i as i64) - 1).execute(db.pool()).await.unwrap();
    }
    for i in 0..10 {
        sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES(?,?,'coder','completed',?,?)")
            .bind(format!("{id}-e{i}")).bind(id).bind(format!("2026-01-01T00:00:{i:02}Z")).bind("u").execute(db.pool()).await.unwrap();
    }
}

async fn claim(db: &SqliteDb, id: &str) -> db::TaskStep {
    let t = TaskRepo::get_by_id(db, id, false).await.unwrap().unwrap();
    let step_id = db::new_uuid_v4();
    db.enqueue_step(&EnqueueTaskStep {
        id: step_id.clone(),
        task_id: id.into(),
        kind: "command".into(),
        payload_json: "{}".into(),
        causation_step_id: None,
        causation_key: step_id.clone(),
        chain_id: step_id,
        chain_position: 1,
        expected_status: t.status,
        expected_version: t.version,
        expected_epoch: None,
        lane: "fast".into(),
        available_at: db::now_rfc3339(),
    })
    .await
    .unwrap();
    db.claim_step("cost", Some(id), &db::task_writer::lease_deadline())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
#[ignore = "measurement"]
async fn measure_producer_write_cost() {
    let path = std::env::temp_dir().join(format!(
        "task-condition-producer-cost-{}.db",
        std::process::id()
    ));
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
    let pool = db::create_sqlite_pool(&format!("sqlite:{}", path.display()))
        .await
        .unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    // A realistic Project workflow: the default definition is ~tens of KB.
    let workflow = std::env::var("COST_WORKFLOW").unwrap_or_else(|_| "{}".into());
    ProjectRepo::create(
        &db,
        CreateProject {
            id: "p".into(),
            owner_id: None,
            name: "Cost".into(),
            primary_repo_id: None,
            updated_at: db::now_rfc3339(),
            settings: "{}".into(),
            workflow_definition: workflow.clone(),
            created_at: db::now_rfc3339(),
        },
    )
    .await
    .unwrap();
    println!("COST workflow_definition bytes={}", workflow.len());
    mk(&db, "root", None).await;
    mk(&db, "child", Some("root")).await;
    let n = 1000u32;
    for target in ["root", "child"] {
        let step = claim(&db, target).await;
        let step_id = step.id.clone();
        db::task_writer::in_task_step(step, async {
            for round in 0..3 {
                // A. status transition (repository status writer).
                let t = Instant::now();
                for i in 0..n {
                    let task = TaskRepo::get_by_id(&db, target, false).await.unwrap().unwrap();
                    TaskRepo::update_status(
                        &db,
                        UpdateTaskStatus {
                            id: target.into(),
                            expected_version: task.version,
                            status: if i % 2 == 0 { "in_progress" } else { "todo" }.into(),
                            assignee_id: None,
                            error_annotation: None,
                            blocked_json: None,
                            failed_json: None,
                            updated_at: db::now_rfc3339(),
                        },
                    )
                    .await
                    .unwrap();
                }
                let status = t.elapsed() / n;
                // B. budget charge.
                let t = Instant::now();
                for i in 0..n {
                    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
                    db::budget::charge(&mut tx, target, "execution", 1_000_000, &format!("{step_id}-{round}-{i}"))
                        .await
                        .unwrap();
                    tx.commit().await.unwrap();
                }
                let charge = t.elapsed() / n;
                println!("COST {target}[{round}] status-transition={status:?} budget-charge={charge:?}");
            }
            for round in 0..3 {
                let n = 200u32;
                // D. execution admission + nothing else (create, Running then Completed rows).
                let t = Instant::now();
                for i in 0..n {
                    ExecutionRepo::create(
                        &db,
                        CreateExecution {
                            id: format!("{target}-x{round}-{i}"),
                            task_id: target.into(),
                            agent_id: None,
                            role: "coder".into(),
                            status: ExecutionStatus::Completed,
                            stop_reason: None,
                            stopped_by: None,
                            resume_policy: None,
                            stopped_at: None,
                            parent_execution_id: None,
                            agent_session_id: None,
                            agent_message_id: None,
                            last_activity_at: None,
                            summary: None,
                            logs_path: None,
                            before_sha: None,
                            after_sha: None,
                            error: None,
                            executor_config_snapshot_json: None,
                            workspace_id: None,
                            created_at: db::now_rfc3339(),
                            updated_at: db::now_rfc3339(),
                        },
                    )
                    .await
                    .unwrap();
                }
                let exec = t.elapsed() / n;
                println!("COST {target}[{round}] execution-create(settled)={exec:?} (history +200/round)");
            }
            let mut tx = db::begin_immediate(db.pool()).await.unwrap();
            let s = sqlx::query_scalar::<_, String>("SELECT id FROM task_step WHERE task_id=? AND status='claimed'").bind(target).fetch_one(&mut *tx).await.unwrap();
            sqlx::query("UPDATE task_step SET status='done',completed_at='x' WHERE id=?").bind(&s).execute(&mut *tx).await.unwrap();
            tx.commit().await.unwrap();
        })
        .await;
    }
    // C. hooks step life cycle on an unleased Task: enqueue + claim + finish.
    mk(&db, "steps", None).await;
    for round in 0..3 {
        let t = Instant::now();
        for _ in 0..n {
            let id = db::new_uuid_v4();
            let mut tx = db::begin_immediate(db.pool()).await.unwrap();
            db.enqueue_step_in_tx(
                &mut tx,
                &EnqueueTaskStep {
                    id: id.clone(),
                    task_id: "steps".into(),
                    kind: "hooks".into(),
                    payload_json: "{}".into(),
                    causation_step_id: None,
                    causation_key: id.clone(),
                    chain_id: id.clone(),
                    chain_position: 1,
                    expected_status: "todo".into(),
                    expected_version: 1,
                    expected_epoch: None,
                    lane: "fast".into(),
                    available_at: db::now_rfc3339(),
                },
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();
            let step = db
                .claim_step("cost", Some("steps"), &db::task_writer::lease_deadline())
                .await
                .unwrap()
                .unwrap();
            let mut tx = db::begin_immediate(db.pool()).await.unwrap();
            db.finish_step_in_tx(&mut tx, &step, "done", None)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            // As the worker does: a settled step's lease no longer holds the Task.
            db.release_step(&step.id, "cost").await.unwrap();
        }
        println!(
            "COST steps[{round}] hooks-enqueue+claim+finish={:?}",
            t.elapsed() / n
        );
    }
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}
