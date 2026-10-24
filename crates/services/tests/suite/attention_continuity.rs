#[path = "../../../db/tests/support/journal_probe.rs"]
mod support;

#[tokio::test]
async fn native_checkpoint_presence() {
    let db = support::fixture().await;
    let service = services::AttentionService::new(db.clone(), crate::noop_wake_turns());
    let detail = service
        .agent_detail("probe-owner", "probe-agent", 20)
        .await
        .unwrap();
    assert!(!detail.continuity.checkpoint_present);
    support::native(&db, "protected", false, false, false).await;
    assert!(
        !service
            .agent_detail("probe-owner", "probe-agent", 20)
            .await
            .unwrap()
            .continuity
            .checkpoint_present
    );
    support::legacy(&db, true).await;
    assert!(
        service
            .agent_detail("probe-owner", "probe-agent", 20)
            .await
            .unwrap()
            .continuity
            .checkpoint_present
    );
    support::native(&db, "protected", true, false, false).await;
    assert!(
        !service
            .agent_detail("probe-owner", "probe-agent", 20)
            .await
            .unwrap()
            .continuity
            .checkpoint_present
    );
    support::native(&db, "protected", true, true, false).await;
    assert!(
        service
            .agent_detail("probe-owner", "probe-agent", 20)
            .await
            .unwrap()
            .continuity
            .checkpoint_present
    );
    support::native(&db, "protected", true, false, true).await;
    assert!(
        !service
            .agent_detail("probe-owner", "probe-agent", 20)
            .await
            .unwrap()
            .continuity
            .checkpoint_present
    );
}
