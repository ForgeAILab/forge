use super::*;

/// m2 (port of `audit_size_scaling`'s concern): the stored condition is
/// bounded whatever the row holds, and an unrelated large metadata key is
/// neither copied nor needed.
#[tokio::test]
async fn evidence_is_bounded_and_large_unrelated_fields_are_not_copied() {
    let big = "m".repeat(1_000_000);
    let legacy = ConditionInput {
        error_annotation: Some(json!({"type":"workspace_error","message":big}).to_string()),
        blocked_json: Some(json!({"kind":"workspace_error","reason":big}).to_string()),
        metadata_json: Some(json!({"custom":big,"owner_wait":{"daemon_id":"d","snapshot":big},"last_workflow_guard_reason":big}).to_string()),
        ..Default::default()
    };
    let condition = compose_condition(&legacy);
    let stored = encode(&condition);
    assert!(
        stored.len() < 6 * (EVIDENCE_VALUE_LIMIT + 64),
        "{}",
        stored.len()
    );
    assert!(!stored.contains("custom"));
    let TaskCondition::Parked { evidence, .. } = &condition else {
        panic!("{condition:?}")
    };
    let annotation = evidence.error_annotation.as_deref().unwrap();
    assert!(annotation.ends_with(&format!(
        "…[truncated; {} bytes]",
        legacy.error_annotation.as_ref().unwrap().len()
    )));
    assert!(evidence.metadata["owner_wait"].contains("[truncated; "));
    assert!(reasons(&condition)
        .iter()
        .any(|r| matches!(r, ParkReason::OwnerOffline { daemon_id: Some(id), .. } if id == "d")));
    // Truncation never splits a character.
    let wide = ConditionInput {
        blocked_json: Some(format!(
            "{{\"kind\":\"x\",\"reason\":\"{}\"}}",
            "\u{06A9}".repeat(40_000)
        )),
        ..Default::default()
    };
    assert_eq!(
        decode(&encode(&compose_condition(&wide))).unwrap(),
        compose_condition(&wide)
    );

    // The legacy columns keep the full data.
    let db = db().await;
    let t = task(&db, "big").await;
    let step = claim(&db, &t.id).await;
    crate::task_writer::in_task_step(step, async {
        let mut u = update(&t);
        u.error_annotation = Some(legacy.error_annotation.clone());
        let updated = TaskRepo::update(&db, u).await.unwrap();
        assert_eq!(updated.error_annotation, legacy.error_annotation);
        db.check_task_condition_invariant(&updated).await.unwrap();
    })
    .await;
    let cached: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id='big'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(
        decode(&cached)
            .unwrap()
            .evidence()
            .channel_digest
            .as_ref()
            .unwrap()
            .len(),
        64
    );
    let length = test_support::without_channel_digest(&cached).len() as i64;
    assert!(length < 2 * EVIDENCE_VALUE_LIMIT as i64, "{length}");
}
