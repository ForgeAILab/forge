use super::*;
use crate::recovery::test_support::{conditions::*, seed_project_repo, seed_task, sqlite_db};

#[tokio::test]
async fn typed_executor_failure_matches_archived_main_for_every_shape_and_second_write() {
    for shape in SHAPES {
        let db = Arc::new(sqlite_db().await);
        let projector = attention_projector(&db);
        let (project, _) = seed_project_repo(&db).await;
        let task = seed_task(&db, project, "in_progress".into(), None).await;
        let execution = db::ExecutionRepo::create(
            &*db,
            db::CreateExecution {
                id: "fixture-execution".into(),
                task_id: task.id.clone(),
                agent_id: None,
                role: "coder".into(),
                status: db::ExecutionStatus::Failed,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: Some("execution.jsonl".into()),
                before_sha: None,
                after_sha: None,
                error: Some(if *shape == "long_reason" {
                    "z".repeat(200_000)
                } else {
                    format!("executor failure: {shape}")
                }),
                executor_config_snapshot_json: None,
                workspace_id: None,
                created_at: "2026-10-25T00:00:00Z".into(),
                updated_at: "2026-10-25T00:00:00Z".into(),
            },
        )
        .await
        .unwrap();
        let service = TaskService::new(db.clone(), Arc::new(events::EventBus::default()));
        let step = claim(&db, &task).await;
        db::task_writer::in_task_step(step.clone(), async {
            seed_shape(&db, &task, shape).await;
            let before = read(&db, &task.id).await;
            service
                .block_task_after_executor_failure(&before, &execution)
                .await
                .unwrap();
            let row = read(&db, &task.id).await;
            assert!(row.failed_json.is_none());
            assert_main_snapshot(&db, &projector, &row, "executor", shape, "first").await;
            assert_next_write(&db, &projector, &row, "executor", shape, &step).await;
        })
        .await;
    }
}
