use crate::worker_runtime::queue::writer_parity::case;

#[tokio::test]
async fn plan_wait() {
    case!(
        "plan-wait",
        "in_progress",
        f,
        {
            let execution = f.execution(db::ExecutionStatus::Failed).await;
            super::record_retry(
                &f.db,
                &execution.id,
                "read",
                "daemon",
                &crate::workspace_backend::WorkspaceBackendError::OwnerUnreachable {
                    daemon_id: "daemon".into(),
                },
            )
            .await
            .unwrap();
        },
        |row, raw| {
            let metadata: serde_json::Value =
                serde_json::from_str(row.metadata_json.as_deref().unwrap()).unwrap();
            let started = metadata["owner_wait"]["started_at"].as_str().unwrap();
            let retry = metadata["plan_settlement_wait"]["retry_at"]
                .as_str()
                .unwrap();
            assert_eq!(
                chrono::DateTime::parse_from_rfc3339(retry).unwrap()
                    - chrono::DateTime::parse_from_rfc3339(started).unwrap(),
                chrono::Duration::seconds(5)
            );
            let raw = raw
                .replace(started, "<transport-now>")
                .replace(retry, "<transport-retry>");
            // The event digest includes the uncontrolled transport times.
            let mut fields: serde_json::Value = serde_json::from_str(&raw).unwrap();
            fields.as_object_mut().unwrap().remove("event_digest");
            fields.to_string()
        }
    );
}
