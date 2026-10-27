use crate::worker_runtime::queue::writer_parity::case;

#[tokio::test]
async fn lifecycle_annotations() {
    for timeout in [false, true] {
        case!(&format!("script-hook-{timeout}"), "in_progress", f, {
            let mut failure = crate::worker_runtime::queue::writer_parity::hook_failure();
            failure.timed_out = timeout;
            super::annotate_before_work_hook_block(&f.ctx, &f.task, &failure)
                .await
                .unwrap();
        });
    }
    case!("lifecycle-reset", "in_progress", f, {
        super::annotate_before_work_workspace_reset(
            &f.ctx,
            &f.task,
            &crate::ServiceError::invalid_operation("reset"),
        )
        .await
        .unwrap();
    });
}
