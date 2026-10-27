use crate::recovery::test_support::writer_fixture as fixture;
use db::TaskStepRepo;
use fixture::{after, Fixture, SnapshotOptions};

#[tokio::test]
async fn coordination_writers_match_main() {
    let f = Fixture::new("in_progress").await;
    db::task_writer::in_task_step(f.step.clone(), async {
        f.ctx
            .task_service
            .set_coordination_review_pending(&f.task.id, true)
            .await
            .unwrap();
        after(&f, "coordination-metadata", SnapshotOptions::default()).await;
    })
    .await;
    drop(f);
    // The same flag/identity constructors also run through the queued mutation seam.
    let f = Fixture::new("in_progress").await;
    let mut tx = f.db.pool().begin().await.unwrap();
    f.db.finish_step_in_tx(&mut tx, &f.step, "done", None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    f.db.release_step(&f.step.id, f.step.claimed_by.as_deref().unwrap())
        .await
        .unwrap();
    f.ctx
        .task_service
        .set_coordination_review_pending(&f.task.id, true)
        .await
        .unwrap();
    f.ctx.task_service.drain(&f.task.id).await.unwrap();
    fixture::snapshot(
        &f,
        "coordination-queued",
        "first",
        SnapshotOptions::default(),
    )
    .await;
    f.ctx
        .task_service
        .set_coordination_review_pending(&f.task.id, false)
        .await
        .unwrap();
    f.ctx.task_service.drain(&f.task.id).await.unwrap();
    fixture::snapshot(
        &f,
        "coordination-queued",
        "following-unconverted-clear",
        SnapshotOptions::default(),
    )
    .await;
}
