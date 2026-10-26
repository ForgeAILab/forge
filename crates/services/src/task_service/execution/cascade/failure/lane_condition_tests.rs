use crate::worker_runtime::queue::lane_conditions::case;

#[tokio::test]
async fn unavailable_and_retry() {
    for (name, attempts) in [
        (
            "typed",
            serde_json::json!([{"candidate_key":"candidate","outcome":"unavailable"}]),
        ),
        ("null", serde_json::Value::Null),
        ("historical", serde_json::json!([{"custom":"kept"}])),
    ] {
        case!(&format!("executor-unavailable-{name}"), "in_progress", f, {
            let execution = f.execution(db::ExecutionStatus::Failed).await;
            f.ctx
                .task_service
                .annotate_executor_unavailable_block(&execution, None, attempts.clone())
                .await
                .unwrap();
        });
    }
    case!("retry-metadata", "in_progress", f, {
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
    });
}
