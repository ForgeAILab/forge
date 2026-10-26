use crate::worker_runtime::queue::lane_conditions::case;

#[tokio::test]
async fn owner_recovery() {
    case!("owner-recovery", "in_progress", f, {
        let execution = f.execution(db::ExecutionStatus::Failed).await;
        super::annotate_owner_recovery(
            &f.db,
            &execution,
            &db::PlacementFailureCause::OwnerLostExecution,
        )
        .await
        .unwrap();
    });
}
