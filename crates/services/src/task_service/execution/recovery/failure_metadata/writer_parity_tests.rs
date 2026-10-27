use crate::worker_runtime::queue::writer_parity::case;

#[tokio::test]
async fn failed_metadata() {
    for kind in [None, Some(api_types::FailureKind::InternalCommandFailed)] {
        case!(
            &format!("failed-kind-{}", kind.is_some()),
            "in_progress",
            f,
            {
                f.ctx
                    .task_service
                    .fail_task(&f.task.id, "failed", kind, None)
                    .await
                    .unwrap();
            }
        );
    }
}
