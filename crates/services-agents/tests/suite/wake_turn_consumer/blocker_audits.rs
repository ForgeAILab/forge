//! Wake turn consumer blocker audits tests.

use super::*;

/// Focus 2: a digest change must make the blocker eligible again.
#[tokio::test]
async fn audit35_changed_blocker_digest_after_an_admitted_turn_is_admitted_again() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "execution_failed", "first-cause").await;
    let service = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    audit35_finish_turns(&f).await;
    let changed = audit35_reproject(&f, &a, "a different, new cause").await;
    assert_eq!(changed.id, a.id, "same dedupe key keeps the Attention row");
    assert_eq!(changed.status, "open");
    assert_ne!(
        wake_attention_incident_digest(&changed),
        wake_attention_incident_digest(&a)
    );
    let later = (chrono::Utc::now() + chrono::Duration::minutes(10)).to_rfc3339();
    let admitted = service.sweep_once_at(&later).await.unwrap();
    assert_eq!(
        (admitted, turns(&f).await, audit35_escalations(&f).await),
        (1, 2, 0),
        "changed blocker is neither re-woken nor escalated: stranded"
    );
}

/// Focus 4/8: a blocker turn that failed before the agent could act consumes
/// the digest's only turn and goes straight to the owner.
#[tokio::test]
async fn audit35_failed_blocker_turn_does_not_consume_the_digest() {
    let f = chat_turn_fixture().await;
    incident(&f, "execution_failed", "provider-down").await;
    let service = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    AgentChatTurnWorker::with_runner(f.db.clone(), Arc::new(FailingWakeRunner))
        .run_once()
        .await
        .unwrap();
    let later = (chrono::Utc::now() + chrono::Duration::hours(2)).to_rfc3339();
    service.sweep_once_at(&later).await.unwrap();
    assert_eq!(
        (turns(&f).await, audit35_escalations(&f).await),
        (2, 0),
        "a failed (never-acted) blocker turn is treated as the digest's one turn"
    );
}

/// Focus 4: a recorded recovery outcome should stop the automatic escalation.
#[tokio::test]
async fn audit35_recorded_recovery_outcome_prevents_owner_escalation() {
    let f = chat_turn_fixture().await;
    incident(&f, "environment_not_ready", "env").await;
    let service = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    let turn_id: String = sqlx::query_scalar("SELECT id FROM agent_chat_turn_job WHERE chat_id=?")
        .bind(&f.chat_id)
        .fetch_one(f.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE agent_wake_blocker SET recorded_outcome='recovery_action'")
        .execute(f.db.pool())
        .await
        .unwrap();
    audit35_finish_turns(&f).await;
    services::project_escalation::ProjectEscalationService::new(f.db.clone())
        .escalate_silent_turn(&turn_id)
        .await
        .unwrap();
    assert_eq!(
        audit35_escalations(&f).await,
        0,
        "silent path honours the outcome"
    );
    service
        .sweep_once_at(&(chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339())
        .await
        .unwrap();
    assert_eq!(
        audit35_escalations(&f).await,
        0,
        "sweep escalates even though the turn recorded a recovery action"
    );
}

/// Focus 3: wake_budget=3 leaves the decision bucket at 0, so an owner answer never wakes.
#[tokio::test]
async fn audit35_small_wake_budget_still_wakes_on_owner_answer() {
    let f = chat_turn_fixture().await;
    sqlx::query(
        "UPDATE project_agent_binding SET wake_budget=3 WHERE project_id=? AND state='active'",
    )
    .bind(&f.project_id)
    .execute(f.db.pool())
    .await
    .unwrap();
    let service = services::project_escalation::ProjectEscalationService::new(f.db.clone());
    let e = service
        .escalate(
            &f.project_id,
            Agent(&f.identity_id),
            api_types::ProjectEscalateRequest {
                need: "Need the staging token".to_owned(),
                task_ids: vec![],
            },
            "k",
        )
        .await
        .unwrap();
    service
        .answer(
            &f.project_id,
            &e.id,
            &f.account_id,
            api_types::AnswerProjectEscalationRequest {
                expected_version: 1,
                answer: "Token added".to_owned(),
            },
        )
        .await
        .unwrap();
    AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    )
    .project_once(100)
    .await
    .unwrap();
    let stalled: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type='notification.requested' AND scope_id=?",
    )
    .bind(&f.project_id)
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert_eq!(
        (turns(&f).await, stalled),
        (1, 0),
        "owner answer dropped and a false 'Project Agent stopped' notice raised"
    );
}

/// Focus 3: a decision wake suppressed by its 2/h bucket is never reconsidered.
/// Ported: owner answers are uncharged (M3), so the bucket is driven by user
/// decisions; the owner-answer half is asserted on its own.
#[tokio::test]
async fn audit35_budget_suppressed_owner_answer_is_reconsidered() {
    let f = chat_turn_fixture().await;
    let attention = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    let mut results = Vec::new();
    for n in 0..3 {
        let decision = incident(&f, "decision_recorded", &format!("decision-{n}")).await;
        results.push(
            attention
                .admit_wake(wake_request(&f, &decision, &now_rfc3339()))
                .await
                .unwrap(),
        );
        audit35_finish_turns(&f).await;
    }
    assert!(matches!(
        results[2],
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::BudgetExhausted
        }
    ));
    assert_eq!(
        turns(&f).await,
        2,
        "third decision suppressed by the 2/h bucket"
    );
    attention
        .sweep_once_at(&(chrono::Utc::now() + chrono::Duration::hours(2)).to_rfc3339())
        .await
        .unwrap();
    assert_eq!(
        turns(&f).await,
        3,
        "the suppressed decision is never delivered"
    );
    // Owner answers never draw on that bucket.
    let service = services::project_escalation::ProjectEscalationService::new(f.db.clone());
    for n in 0..3 {
        let e = service
            .escalate(
                &f.project_id,
                Agent(&f.identity_id),
                api_types::ProjectEscalateRequest {
                    need: format!("Need {n}"),
                    task_ids: vec![],
                },
                &format!("k{n}"),
            )
            .await
            .unwrap();
        service
            .answer(
                &f.project_id,
                &e.id,
                &f.account_id,
                api_types::AnswerProjectEscalationRequest {
                    expected_version: 1,
                    answer: format!("Answer {n}"),
                },
            )
            .await
            .unwrap();
        attention.project_once(100).await.unwrap();
        audit35_finish_turns(&f).await;
    }
    assert_eq!(turns(&f).await, 6, "every owner answer wakes the Agent");
}

/// Focus 1/5: upgrade path. An imported admitted digest whose Attention source
/// is unchanged is rekeyed and escalated on the first sweep (one notice per row).
#[tokio::test]
async fn audit35_imported_admitted_blocker_escalates_on_first_sweep() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "execution_failed", "legacy").await;
    let service = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    audit35_finish_turns(&f).await;
    // Shape of a row the V202610042145 import writes for a pre-upgrade turn.
    sqlx::query(
        "UPDATE agent_wake_blocker SET incident_digest='legacy-format', legacy_source_event_id=?",
    )
    .bind(&a.source_event_id)
    .execute(f.db.pool())
    .await
    .unwrap();
    service
        .sweep_once_at(&(chrono::Utc::now() + chrono::Duration::minutes(10)).to_rfc3339())
        .await
        .unwrap();
    // Ported to the owner's M5 rule: an imported blocker gets one normal
    // re-admission and no escalation.
    assert_eq!(
        (turns(&f).await, audit35_escalations(&f).await),
        (2, 0),
        "first post-upgrade sweep escalates every imported unchanged blocker"
    );
}

/// Focus 5: a busy chat is audited as a missing responder binding.
#[tokio::test]
async fn audit35_busy_chat_is_not_audited_as_missing_binding() {
    let f = chat_turn_fixture().await;
    let service = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    let a = incident(&f, "delivery_followup", "first").await;
    service
        .admit_wake(wake_request(&f, &a, &now_rfc3339()))
        .await
        .unwrap();
    let b = incident(&f, "delivery_followup", "second").await;
    let result = service
        .admit_wake(wake_request(&f, &b, &now_rfc3339()))
        .await
        .unwrap();
    assert!(
        !matches!(result, services::WakeAdmissionResult::SetupRequired { .. }),
        "busy chat recorded as {result:?}"
    );
}

/// Focus 1: a non-first batch member re-arms while the batch turn is still queued.
#[tokio::test]
async fn audit35_second_batch_member_is_not_rewoken_while_batch_turn_is_queued() {
    let f = chat_turn_fixture().await;
    let _a = incident(&f, "execution_failed", "one").await;
    let b = incident(&f, "review_risk", "two").await;
    let service = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    // Batch turn is still queued; B's material state changes.
    audit35_reproject(&f, &b, "review risk changed").await;
    service
        .sweep_once_at(&(chrono::Utc::now() + chrono::Duration::minutes(10)).to_rfc3339())
        .await
        .unwrap();
    assert_eq!(
        (turns(&f).await, charged(&f, "blocker").await),
        (1, 1),
        "second turn admitted for an Attention whose batch turn has not run"
    );
}

#[tokio::test]
async fn conflict_hotspot_wake_routes_to_project_agent_with_bounded_directive() {
    let fixture = chat_turn_fixture().await;
    fixture
        .db
        .append_event(CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.conflict_hotspot.detected".into(),
            entity_type: "project".into(),
            entity_id: fixture.project_id.clone(),
            actor_type: "system".into(),
            actor_id: Some("conflict-hotspots".into()),
            scope_type: "project".into(),
            scope_id: fixture.project_id.clone(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(new_uuid_v4()),
            payload_json:
                serde_json::json!({"project_id": fixture.project_id, "path": "src/共有.rs",
            "task_ids": ["task-3", "task-2", "task-1"], "handoff_count": 3, "window_days": 7})
                .to_string(),
            created_at: now_rfc3339(),
        })
        .await
        .unwrap();
    AttentionService::new(
        Arc::clone(&fixture.db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&fixture.db))),
    )
    .project_once(100)
    .await
    .unwrap();
    let admitted: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_event WHERE event_type = 'agent.wake.admitted' AND scope_type = 'project' AND scope_id = ?")
        .bind(&fixture.project_id).fetch_one(fixture.db.pool()).await.unwrap();
    assert_eq!(admitted, 1);
    let (chat_id, responder, content): (String, String, String) = sqlx::query_as(
        "SELECT j.chat_id, j.responder_identity_id, m.content FROM agent_chat_turn_job j
         JOIN agent_chat_message m ON m.id = j.triggering_message_id
         WHERE j.chat_id = ? AND m.content LIKE '%Category: conflict_hotspot%'",
    )
    .bind(&fixture.chat_id)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    assert_eq!(chat_id, fixture.chat_id);
    assert_eq!(responder, fixture.identity_id);
    let directive = content.lines().last().unwrap();
    assert!(content.contains("src/共有.rs"));
    for task in ["task-1", "task-2", "task-3"] {
        assert_eq!(content.matches(task).count(), 1);
    }
    assert!(directive.contains("path and Tasks in Details"));
    assert!(directive.contains("Propose one Task via `task.propose`"));
    assert!(directive.contains("unless an open Task already does"));
    assert!(!directive.contains("Resolve"));
    assert!(directive.split_whitespace().count() <= 40);
}

/// Two adjacent handoffs at/above threshold produce one stable detection and
/// exactly one Project Agent turn.
#[tokio::test]
async fn audit39b_refresh_before_delivery_loses_the_only_wake() {
    let fixture = chat_turn_fixture().await;
    audit39b_detection(&fixture, "src/shared.rs", 3).await;
    audit39b_detection(&fixture, "src/shared.rs", 4).await;
    AttentionService::new(
        Arc::clone(&fixture.db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&fixture.db))),
    )
    .project_once(100)
    .await
    .unwrap();
    let detections: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'project.conflict_hotspot.detected'",
    )
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    assert_eq!(detections, 1);
    assert_eq!(
        audit39b_hotspot_turns(&fixture).await,
        1,
        "open conflict_hotspot incident produced no Project Agent turn"
    );
}

#[tokio::test]
async fn audit39b_each_refresh_after_cooldown_rewakes_project_agent() {
    let fixture = chat_turn_fixture().await;
    let attention = AttentionService::new(
        Arc::clone(&fixture.db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&fixture.db))),
    );
    audit39b_detection(&fixture, "src/shared.rs", 3).await;
    attention.project_once(100).await.unwrap();
    for count in 4..=20 {
        // Simulate the 300 s cooldown elapsing between two later handoffs.
        sqlx::query("UPDATE agent_wake_lease SET leased_until = '2000-01-01T00:00:00Z', cooldown_until = '2000-01-01T00:00:00Z'")
            .execute(fixture.db.pool()).await.unwrap();
        audit39b_detection(&fixture, "src/shared.rs", count).await;
        attention.project_once(100).await.unwrap();
    }
    let admitted: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'agent.wake.admitted'",
    )
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    let budget: Option<i64> =
        sqlx::query_scalar("SELECT MAX(admitted_count) FROM agent_wake_budget_window")
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    assert_eq!(audit39b_hotspot_turns(&fixture).await, 1);
    assert_eq!(budget, Some(1));
    assert_eq!(
        admitted, 1,
        "every refresh after cooldown re-woke the Project Agent for the same open incident"
    );
}
