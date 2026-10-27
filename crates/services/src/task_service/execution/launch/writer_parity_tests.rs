use crate::worker_runtime::queue::writer_parity::case;

#[tokio::test]
async fn stop_execution() {
    case!("manual-stop", "in_progress", f, {
        let execution = f.execution(db::ExecutionStatus::Running).await;
        f.ctx
            .task_service
            .stop_execution(&execution.id, "stop".into())
            .await
            .unwrap();
    });
}
