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

use crate::recovery::test_support::writer_fixture::{case, channel_sample};

#[tokio::test]
async fn queue_channels_and_overlays() {
    for (kind, status, error) in [
        (
            api_types::FailureKind::CascadeFailed,
            "failed",
            Some("failed"),
        ),
        (
            api_types::FailureKind::WorkspaceError,
            "failed",
            Some("workspace"),
        ),
        (api_types::FailureKind::WorkspaceError, "failed", None),
        (api_types::FailureKind::WorkflowLoop, "parked", None),
        (
            api_types::FailureKind::MergeConflict,
            "failed",
            Some("merge"),
        ),
        (api_types::FailureKind::CiFailed, "failed", Some("ci")),
    ] {
        case!(
            &format!("step-{kind}-{}", error.is_some()),
            "in_progress",
            f,
            {
                let worker = super::TaskStepWorker::new(f.ctx.task_service.clone());
                let mut tx = f.db.pool().begin().await.unwrap();
                let mut task = f.task.clone();
                worker
                    .write_annotation_in_tx(&mut tx, &f.step, &mut task, status, kind, error)
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
            }
        );
    }
    for sample in [
        "executor",
        "hold",
        "script-hook",
        "dispatch-state",
        "dispatch-upgrade",
        "lifecycle-reset",
        "owner-recovery",
        "removed-machine",
        "recovered-task",
        "budget",
        "review-finding",
        "workflow-guard",
        "review-candidate",
        "review-ci",
        "placement",
        "plan-wait",
        "review-park-null",
        "historical",
    ] {
        case!(&format!("overlay-{sample}"), "in_progress", f, {
            let raw = if sample == "historical" {
                "{\"type\":\"unknown\",\"extra\":true}".into()
            } else {
                channel_sample(sample)
            };
            sqlx::query("UPDATE task SET error_annotation=?,blocked_json='{}' WHERE id=?")
                .bind(raw)
                .bind(&f.task.id)
                .execute(f.db.pool())
                .await
                .unwrap();
            let mut task = db::TaskRepo::get_by_id(&*f.db, &f.task.id, false)
                .await
                .unwrap()
                .unwrap();
            let worker = super::TaskStepWorker::new(f.ctx.task_service.clone());
            let mut tx = f.db.pool().begin().await.unwrap();
            worker
                .write_annotation_in_tx(
                    &mut tx,
                    &f.step,
                    &mut task,
                    "failed",
                    api_types::FailureKind::CascadeFailed,
                    Some("later hook failed"),
                )
                .await
                .unwrap();
            tx.commit().await.unwrap();
        });
    }
}
