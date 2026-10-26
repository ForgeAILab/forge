use crate::worker_runtime::queue::lane_conditions::case;

#[tokio::test]
async fn service_hook() {
    for timeout in [false, true] {
        case!(
            &format!("service-script-hook-{timeout}"),
            "in_progress",
            f,
            {
                let mut failure = crate::worker_runtime::queue::lane_conditions::hook_failure();
                failure.timed_out = timeout;
                f.ctx
                    .task_service
                    .annotate_before_work_hook_block(&f.task, &failure)
                    .await
                    .unwrap();
            }
        );
    }
}
