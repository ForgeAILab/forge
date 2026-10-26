use crate::worker_runtime::queue::lane_conditions::case;

#[tokio::test]
async fn review_finding() {
    case!("review-finding", "review", f, {
        let execution = f.execution(db::ExecutionStatus::Failed).await;
        f.review(&execution).await;
        f.ctx
            .task_service
            .block_task_for_review_finding(
                &f.task,
                &execution,
                api_types::FailureKind::ReviewNeedsOwner,
                "finding".into(),
            )
            .await
            .unwrap();
    });
}
