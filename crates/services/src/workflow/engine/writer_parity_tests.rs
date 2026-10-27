use crate::worker_runtime::queue::writer_parity::case;

#[tokio::test]
async fn dispatch_annotations() {
    case!("dispatch-state", "in_progress", f, {
        super::annotate_dispatch_failure(
            &f.db,
            &f.task.id,
            &f.task.status,
            "dispatch refused",
            None,
        )
        .await
        .unwrap();
    });
    case!("dispatch-upgrade", "in_progress", f, {
        super::annotate_upgrade_dispatch_refusal(
            &f.db,
            &f.task.id,
            &f.task.status,
            &crate::ServiceError::DaemonUpgradeRequired {
                daemon_id: "daemon".into(),
            },
        )
        .await
        .unwrap();
    });
    case!("upgrade-carried", "in_progress", f, {
        db::TaskRepo::mutate_metadata(
            &*f.db,
            &f.task.id,
            None,
            vec![
                db::TaskMetadataMutation::Set {
                    key: "deferred_dispatch".into(),
                    value: serde_json::json!({"caller_backoff":{"values":[1,true,null]}}),
                },
                db::TaskMetadataMutation::Set {
                    key: "dispatch_disposition".into(),
                    value: serde_json::json!({"caller_refusal":true}),
                },
            ],
            &db::now_rfc3339(),
        )
        .await
        .unwrap();
        sqlx::query("UPDATE task SET error_annotation=? WHERE id=?")
            .bind(
                serde_json::json!({"type":"manual_stop","caller_extension":[1,true,null]})
                    .to_string(),
            )
            .bind(&f.task.id)
            .execute(f.db.pool())
            .await
            .unwrap();
        super::annotate_upgrade_dispatch_refusal(
            &f.db,
            &f.task.id,
            &f.task.status,
            &crate::ServiceError::DaemonUpgradeRequired {
                daemon_id: "daemon".into(),
            },
        )
        .await
        .unwrap();
    });
}
