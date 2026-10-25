use super::*;

/// Runs the production merge against the annotation written by the real service.
pub(crate) async fn merge_after_writer(
    db: &db::SqliteDb,
    row: &db::Task,
    step: &TaskStep,
) -> db::Task {
    let worker = TaskStepWorker::new(crate::TaskService::new(
        Arc::new(db.clone()),
        Arc::new(events::EventBus::default()),
    ));
    let mut task = row.clone();
    let mut tx = db.pool().begin().await.unwrap();
    worker
        .write_annotation_in_tx(
            &mut tx,
            step,
            &mut task,
            "failed",
            api_types::FailureKind::CascadeFailed,
            Some("later hook failed"),
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();
    // Literal main queue algorithm; this also checks struct-to-sorted reordering.
    let mut expected: serde_json::Value =
        serde_json::from_str(row.error_annotation.as_ref().unwrap()).unwrap();
    expected["task_step_id"] = serde_json::json!(step.id);
    expected["task_step_error"] = serde_json::json!("later hook failed");
    assert_eq!(
        task.error_annotation.as_deref(),
        Some(expected.to_string().as_str())
    );
    task
}
