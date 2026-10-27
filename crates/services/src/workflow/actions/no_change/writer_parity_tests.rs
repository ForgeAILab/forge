use crate::recovery::test_support::writer_fixture::case;

#[tokio::test]
async fn review_park() {
    for execution_id in [Some("execution".to_owned()), None] {
        case!(
            &format!("review-park-{}", execution_id.is_some()),
            "review",
            f,
            {
                let execution = f.execution(db::ExecutionStatus::Failed).await;
                f.review(&execution).await;
                let mut ctx = f.ctx.clone();
                ctx.execution_id = execution_id.clone();
                super::park_no_change(&ctx, &f.task, &f.step).await.unwrap();
            }
        );
    }
}
