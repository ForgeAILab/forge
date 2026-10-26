use crate::worker_runtime::queue::lane_conditions::case;

#[tokio::test]
async fn plan_wait() {
    case!("plan-wait", "in_progress", f, {
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
    });
}
