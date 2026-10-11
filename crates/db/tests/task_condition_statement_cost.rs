//! Refactor 3.1 stage 5a write-cost measurement (not a correctness test):
//! the owner's hold and release, with the condition stated by the writer and
//! with it mapped back from the legacy fields the same write stores.
//! `cargo test --release -p db --test task_condition_statement_cost -- --ignored --nocapture`
use db::{
    ConditionStatement, CreateProject, CreateTask, ProjectRepo, SqliteDb, TaskMetadataMutation,
    TaskRepo,
};
use serde_json::json;
use std::time::Instant;

const AT: &str = "2026-10-07T12:00:00Z";
const TASKS: usize = 200;
const ROUNDS: usize = 5;

async fn write(db: &SqliteDb, id: &str, hold: bool, stated: bool) {
    let version = TaskRepo::get_by_id(db, id, false)
        .await
        .unwrap()
        .unwrap()
        .version;
    let reason = "Wait for the owner measurements";
    let (annotation, blocked, removes, statement) = if hold {
        (
            Some(ConditionStatement::hold_operator_text("user", reason, AT)),
            Some(json!({"kind":"manual_stop","reason":reason,"created_at":AT}).to_string()),
            [
                "queued_recovery",
                "deferred_dispatch",
                "dispatch_disposition",
                "environment_wait",
                "owner_wait",
            ]
            .into_iter()
            .map(|key| TaskMetadataMutation::Remove { key: key.into() })
            .collect(),
            ConditionStatement::Hold {
                actor: "user".into(),
                reason: reason.into(),
                at: AT.into(),
            },
        )
    } else {
        (None, None, Vec::new(), ConditionStatement::Release)
    };
    TaskRepo::update_recovery_metadata_if_no_running_execution(
        db,
        id,
        version,
        annotation,
        blocked,
        None,
        AT,
        None,
        vec!["coder".into()],
        removes,
        stated.then_some(statement),
    )
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "measurement"]
async fn measure_stated_against_mapped_hold_and_release() {
    let path = std::env::temp_dir().join(format!("statement-cost-{}.db", std::process::id()));
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
    let pool = db::create_sqlite_pool(&format!("sqlite:{}", path.display()))
        .await
        .unwrap();
    db::run_migrations(&pool).await.unwrap();
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
    for i in 0..TASKS {
        TaskRepo::create(
            &db,
            CreateTask {
                id: format!("t{i}"),
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
    // [stated hold, stated release, mapped hold, mapped release], per round.
    let mut micros: [Vec<f64>; 4] = Default::default();
    for round in 0..ROUNDS * 2 {
        // Alternate which side goes first so neither always runs warm.
        let stated = round % 2 == 0;
        for (hold, slot) in [(true, 0), (false, 1)] {
            let started = Instant::now();
            for i in 0..TASKS {
                write(&db, &format!("t{i}"), hold, stated).await;
            }
            let each = started.elapsed().as_secs_f64() * 1e6 / TASKS as f64;
            micros[slot + if stated { 0 } else { 2 }].push(each);
        }
    }
    let median = |values: &mut Vec<f64>| {
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        values[values.len() / 2]
    };
    let [stated_hold, stated_release, mapped_hold, mapped_release] = &mut micros;
    println!(
        "COST hold: stated {:.0} us, mapped {:.0} us; release: stated {:.0} us, mapped {:.0} us",
        median(stated_hold),
        median(mapped_hold),
        median(stated_release),
        median(mapped_release),
    );
    drop(db);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}
