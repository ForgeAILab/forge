use super::*;
use crate::recovery::test_support::{conditions::*, seed_project_repo, seed_task, sqlite_db};

#[tokio::test]
async fn typed_executor_failure_matches_archived_main_for_every_shape_and_second_write() {
    let mut visited = 0;
    for shape in SHAPES {
        let db = Arc::new(sqlite_db().await);
        let _clock = freeze_clock();
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
                logs_path: (*shape != "clear").then(|| "execution.jsonl".into()),
                before_sha: None,
                after_sha: None,
                error: Some(match *shape {
                    "long_reason" => "z".repeat(200_000),
                    "clear" => ESCAPED_REASON.into(),
                    _ => format!("executor failure: {shape}"),
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
        visited += 3;
    }
    assert_writer_cases("executor", visited);
}

use crate::recovery::test_support::writer_fixture::{case, ClockNormalization};

#[tokio::test]
async fn unavailable_and_retry() {
    case!("executor-unavailable-typed", "in_progress", f, {
        let execution = f.execution(db::ExecutionStatus::Failed).await;
        f.ctx
            .task_service
            .annotate_executor_unavailable_block(
                &execution,
                None,
                vec![super::channels::ExecutorRouteAttempt {
                    candidate_key: "candidate".into(),
                    outcome: "unavailable".into(),
                }],
            )
            .await
            .unwrap();
    });
    case!(
        "retry-metadata",
        "in_progress",
        f,
        {
            let before = chrono::Utc::now();
            let execution = f.execution(db::ExecutionStatus::Failed).await;
            let version = serde_json::from_str::<serde_json::Value>(
                execution.executor_config_snapshot_json.as_ref().unwrap(),
            )
            .unwrap()["project_version"]
                .as_i64()
                .unwrap();
            f.ctx
                .task_service
                .maybe_schedule_execution_retry(&execution, &f.task, version, None, None, None)
                .await
                .unwrap();
            let after = chrono::Utc::now();
            let row = db::TaskRepo::get_by_id(&*f.db, &f.task.id, false)
                .await
                .unwrap()
                .unwrap();
            let metadata: serde_json::Value =
                serde_json::from_str(row.metadata_json.as_deref().unwrap()).unwrap();
            let deadline = chrono::DateTime::parse_from_rfc3339(
                metadata["deferred_dispatch"]["not_before"]
                    .as_str()
                    .unwrap(),
            )
            .unwrap()
            .with_timezone(&chrono::Utc);
            assert!(deadline >= before + chrono::Duration::seconds(10));
            assert!(deadline <= after + chrono::Duration::seconds(10));
        },
        ClockNormalization::ExecutionRetry
    );
}
