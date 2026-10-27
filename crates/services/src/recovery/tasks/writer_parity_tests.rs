use crate::worker_runtime::queue::writer_parity::case;

#[tokio::test]
async fn recovered_task() {
    case!("recovered-task", "in_progress", f, {
        f.execution(db::ExecutionStatus::Running).await;
        super::recover_task(
            &f.db,
            f.task.clone(),
            db::StopReason::AgentTimeout,
            &api_types::Actor::system(api_types::SystemComponent::Test),
        )
        .await
        .unwrap();
    });
}
