use super::*;
use crate::recovery::test_support::writer_fixture as fixture;
use fixture::SnapshotOptions;

#[tokio::test]
async fn saved_action_annotations_match_main_including_unknown_fallback() {
    for (name, failure) in [
        (
            "saved-action-known",
            r#"{"created_at":"2026-10-25T00:00:00Z","kind":"executor_failed","reason":"failed"}"#,
        ),
        (
            "saved-action-integration-admission-failed",
            r#"{"kind":"integration_admission_failed","reason":"historical"}"#,
        ),
        ("saved-action-null", r#"{"kind":null,"reason":null}"#),
        (
            "saved-action-unknown",
            r#"{"kind":"unknown","reason":"historical"}"#,
        ),
        (
            "saved-action-extended",
            r#"{"kind":"executor_failed","reason":"historical","caller_extension":[1,null]}"#,
        ),
    ] {
        let f = fixture::Fixture::new("in_progress").await;
        db::task_writer::in_task_step(f.step.clone(), async {
            sqlx::query("UPDATE task SET failed_json=? WHERE id=?")
                .bind(failure)
                .bind(&f.task.id)
                .execute(f.db.pool())
                .await
                .unwrap();
            let mut tx = f.db.pool().begin().await.unwrap();
            f.db.produce_condition_in_tx(&mut tx, &f.task.id, db::ConditionChange::Legacy)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            let actor = api_types::Actor::user(api_types::UserActionSource::Api);
            let snapshot = f
                .ctx
                .task_service
                .task_action_snapshot(&f.task.id, &actor)
                .await
                .unwrap();
            let action = api_types::TaskAction::Restart { reason: None };
            let offer = api_types::Offer {
                action: action.clone(),
                parameters: vec![],
                authority: vec![],
                reason: "historical".into(),
                label: "Restart".into(),
                target_execution_id: None,
                propagates: false,
            };
            f.ctx
                .task_service
                .queue_task_action_as(&snapshot, offer, action, actor, false)
                .await
                .unwrap();
            let row = f.read().await;
            let metadata: Value =
                serde_json::from_str(row.metadata_json.as_deref().unwrap()).unwrap();
            if matches!(
                name,
                "saved-action-unknown" | "saved-action-integration-admission-failed"
            ) {
                assert!(metadata["queued_recovery"]["error_annotation"]
                    .as_str()
                    .unwrap()
                    .contains(r#""type":"unknown""#));
            }
            fixture::after(&f, name, SnapshotOptions::default()).await;
        })
        .await;
    }
}

#[tokio::test]
async fn hold_writers_match_main() {
    for dependency in [false, true] {
        let f = fixture::Fixture::new("in_progress").await;
        db::task_writer::in_task_step(f.step.clone(), async {
            if dependency {
                f.ctx
                    .task_service
                    .block_cancelled_dependencies(&f.task, &["dependency".into()])
                    .await
                    .unwrap();
            }
            let actor = api_types::Actor::user(api_types::UserActionSource::Api);
            let snapshot = f
                .ctx
                .task_service
                .task_action_snapshot(&f.task.id, &actor)
                .await
                .unwrap();
            f.ctx
                .task_service
                .hold_waiting_task(&snapshot, Some("held"))
                .await
                .unwrap();
            fixture::after(
                &f,
                if dependency {
                    "hold-under-dependency"
                } else {
                    "hold"
                },
                SnapshotOptions::default(),
            )
            .await;
        })
        .await;
    }
}
