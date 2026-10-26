use super::*;
use crate::recovery::test_support::writer_fixture as fixture;
use fixture::SnapshotOptions;

#[tokio::test]
async fn workspace_blockers_match_main() {
    for reset in [false, true] {
        let f = fixture::Fixture::new("in_progress").await;
        let service = std::sync::Arc::new(f.ctx.task_service.clone());
        let dispatcher = TaskDispatcher::new(f.db.clone(), f.ctx.event_bus.clone(), service);
        db::task_writer::in_task_step(f.step.clone(), async {
            let error = ServiceError::invalid_operation("é😀\n\"\\\u{7}");
            if reset {
                dispatcher
                    .block_task_for_workspace_reset(&f.task, &error)
                    .await
                    .unwrap();
            } else {
                dispatcher
                    .block_task_on_workspace_error(&f.task, &error)
                    .await
                    .unwrap();
            }
            fixture::after(
                &f,
                if reset {
                    "dispatcher-reset"
                } else {
                    "dispatcher-error"
                },
                SnapshotOptions::default(),
            )
            .await;
        })
        .await;
    }
}
