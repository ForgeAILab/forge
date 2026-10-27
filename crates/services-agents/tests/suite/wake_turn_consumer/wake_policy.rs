//! Wake turn consumer wake policy tests.

use super::*;

#[tokio::test]
async fn suppressed_blocker_is_readmitted_after_cooldown_without_another_event() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "execution_failed", "cooling").await;
    let now = chrono::Utc::now();
    let service = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    let request = wake_request(&f, &a, &now.to_rfc3339());
    sqlx::query("INSERT INTO agent_wake_lease(identity_id,scope_type,scope_id,incident_key,lease_owner,leased_until,reaction_depth,updated_at,cooldown_until) VALUES(?,'project',?,?,?, ?,0,?,?)")
        .bind(&f.identity_id).bind(&f.project_id).bind(&a.dedupe_key).bind("old-lease").bind((now-chrono::Duration::seconds(1)).to_rfc3339()).bind(now.to_rfc3339()).bind((now+chrono::Duration::seconds(300)).to_rfc3339()).execute(f.db.pool()).await.unwrap();
    assert!(matches!(
        service.admit_wake(request).await.unwrap(),
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::Cooldown
        }
    ));
    assert_eq!(charged(&f, "blocker").await, 0);
    assert_eq!(
        service
            .sweep_once_at(&(now + chrono::Duration::seconds(301)).to_rfc3339())
            .await
            .unwrap(),
        1
    );
    assert_eq!(turns(&f).await, 1);
    assert_eq!(charged(&f, "blocker").await, 1);
}
#[tokio::test]
async fn unchanged_admitted_blocker_escalates_once_after_a_silent_turn() {
    let f = chat_turn_fixture().await;
    incident(&f, "execution_failed", "cannot-fix").await;
    let service = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    let worker = AgentChatTurnWorker::with_runner(f.db.clone(), Arc::new(ProseOnlyWakeRunner));
    assert_eq!(worker.run_once().await.unwrap(), 1);
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_wake_escalation WHERE project_id=?")
            .bind(&f.project_id)
            .fetch_one(f.db.pool())
            .await
            .unwrap();
    assert_eq!(count, 1);
    service
        .sweep_once_at(&(chrono::Utc::now() + chrono::Duration::hours(2)).to_rfc3339())
        .await
        .unwrap();
    let notices: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM notification WHERE project_id=? AND event_type='project.escalated'",
    )
    .bind(&f.project_id)
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert_eq!(notices, 1);
    assert_eq!(turns(&f).await, 1);
}
#[tokio::test]
async fn budget_is_charged_only_with_an_atomic_turn_admission() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "delivery_followup", "ready").await;
    sqlx::query("UPDATE agent_chat SET status='agent_setup_required' WHERE id=?")
        .bind(&f.chat_id)
        .execute(f.db.pool())
        .await
        .unwrap();
    let service = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    let result = service
        .admit_wake(wake_request(&f, &a, &now_rfc3339()))
        .await
        .unwrap();
    assert!(matches!(
        result,
        services::WakeAdmissionResult::SetupRequired { .. }
    ));
    assert_eq!(turns(&f).await, 0);
    assert_eq!(charged(&f, "delivery").await, 0);
    sqlx::query("UPDATE agent_chat SET status='ready' WHERE id=?")
        .bind(&f.chat_id)
        .execute(f.db.pool())
        .await
        .unwrap();
    service
        .admit_wake(wake_request(&f, &a, &now_rfc3339()))
        .await
        .unwrap();
    assert_eq!(turns(&f).await, 1);
    assert_eq!(charged(&f, "delivery").await, 1);
}
#[tokio::test]
async fn delivery_spam_cannot_consume_the_blocker_share() {
    let f = chat_turn_fixture().await;
    let service = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    for n in 0..4 {
        let a = incident(&f, "delivery_followup", &format!("delivery-{n}")).await;
        assert!(matches!(
            service
                .admit_wake(wake_request(&f, &a, &now_rfc3339()))
                .await
                .unwrap(),
            services::WakeAdmissionResult::Admitted { .. }
        ));
        sqlx::query("UPDATE agent_chat_turn_job SET status='succeeded' WHERE chat_id=?")
            .bind(&f.chat_id)
            .execute(f.db.pool())
            .await
            .unwrap();
    }
    let extra = incident(&f, "delivery_followup", "overflow").await;
    assert!(matches!(
        service
            .admit_wake(wake_request(&f, &extra, &now_rfc3339()))
            .await
            .unwrap(),
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::BudgetExhausted
        }
    ));
    let blocker = incident(&f, "execution_failed", "repair").await;
    assert!(matches!(
        service
            .admit_wake(wake_request(&f, &blocker, &now_rfc3339()))
            .await
            .unwrap(),
        services::WakeAdmissionResult::Admitted { .. }
    ));
    assert_eq!(charged(&f, "delivery").await, 4);
    assert_eq!(charged(&f, "blocker").await, 1);
}
#[tokio::test]
async fn two_project_blockers_share_one_wake_and_one_directive() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "execution_failed", "one").await;
    let b = incident(&f, "review_risk", "two").await;
    assert_eq!(
        AttentionService::new(
            f.db.clone(),
            Arc::new(services::WakeTurnConsumer::new(f.db.clone()))
        )
        .sweep_once_at(&now_rfc3339())
        .await
        .unwrap(),
        1
    );
    assert_eq!(turns(&f).await, 1);
    assert_eq!(charged(&f, "blocker").await, 1);
    let content: String = sqlx::query_scalar(
        "SELECT content FROM agent_chat_message WHERE chat_id=? AND outcome='attention_wake'",
    )
    .bind(&f.chat_id)
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert!(content.contains(&a.dedupe_key));
    assert!(content.contains(&b.dedupe_key));
    assert_eq!(
        content.matches("Assess the current state").count()
            + content.matches("EXECUTION FAILURE RECOVERY").count(),
        1
    );
    let linked: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_wake_blocker")
        .fetch_one(f.db.pool())
        .await
        .unwrap();
    assert_eq!(linked, 2);
    let payload:String=sqlx::query_scalar("SELECT payload_json FROM domain_event WHERE event_type='agent.wake.admitted' AND scope_id=?").bind(&f.project_id).fetch_one(f.db.pool()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(payload["reason"], "turn_admitted");
    assert_eq!(payload["admission_phase"], "turn");
    assert!(payload.get("action").is_none());
    let id: String = sqlx::query_scalar("SELECT id FROM agent_chat_turn_job WHERE chat_id=?")
        .bind(&f.chat_id)
        .fetch_one(f.db.pool())
        .await
        .unwrap();
    assert_eq!(payload["turn_job_id"], id);
}
#[tokio::test]
async fn deterministic_provider_failure_is_terminal_on_attempt_one() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "delivery_followup", "capacity").await;
    AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    )
    .admit_wake(wake_request(&f, &a, &now_rfc3339()))
    .await
    .unwrap();
    let worker = AgentChatTurnWorker::with_runner(f.db.clone(), Arc::new(FailingWakeRunner));
    worker.run_once().await.unwrap();
    let (status, count, code): (String, i64, String) = sqlx::query_as(
        "SELECT status,attempt_count,error_code FROM agent_chat_turn_job WHERE chat_id=?",
    )
    .bind(&f.chat_id)
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert_eq!(
        (status, count, code),
        ("failed".to_owned(), 1, "usage_limit".to_owned())
    );
    assert_eq!(worker.run_once().await.unwrap(), 0);
}
#[tokio::test]
async fn restart_lease_expiry_refunds_the_attempt_before_reclaim() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "delivery_followup", "restart").await;
    AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    )
    .admit_wake(wake_request(&f, &a, &now_rfc3339()))
    .await
    .unwrap();
    sqlx::query("UPDATE agent_chat_turn_job SET status='leased',attempt_count=1,invocation_count=1,lease_owner='previous-process',leased_until='2000-01-01T00:00:00Z' WHERE chat_id=?").bind(&f.chat_id).execute(f.db.pool()).await.unwrap();
    AgentChatTurnWorker::with_runner(f.db.clone(), Arc::new(FailingWakeRunner))
        .run_once_at(chrono::Utc::now() + chrono::Duration::seconds(1))
        .await
        .unwrap();
    let (status, count): (String, i64) =
        sqlx::query_as("SELECT status,attempt_count FROM agent_chat_turn_job WHERE chat_id=?")
            .bind(&f.chat_id)
            .fetch_one(f.db.pool())
            .await
            .unwrap();
    assert_eq!(status, "failed");
    assert_eq!(count, 1, "restart did not consume the first attempt");
}
#[tokio::test]
async fn escalate_creates_one_owner_notification_and_attention_then_answer_wakes() {
    let f = chat_turn_fixture().await;
    let service = services::project_escalation::ProjectEscalationService::new(f.db.clone());
    assert!(service
        .escalate(
            &f.project_id,
            Agent("foreign-agent"),
            api_types::ProjectEscalateRequest {
                need: "Need disk space".to_owned(),
                task_ids: vec![]
            },
            "need"
        )
        .await
        .is_err());
    let request = api_types::ProjectEscalateRequest {
        need: "Free disk to restore the Project environment".to_owned(),
        task_ids: vec![],
    };
    let e = service
        .escalate(
            &f.project_id,
            Agent(&f.identity_id),
            request.clone(),
            "need",
        )
        .await
        .unwrap();
    let replay = service
        .escalate(&f.project_id, Agent(&f.identity_id), request, "need")
        .await
        .unwrap();
    assert_eq!(e.id, replay.id);
    assert!(service
        .answer(
            &f.project_id,
            &e.id,
            "foreign-owner",
            api_types::AnswerProjectEscalationRequest {
                expected_version: 1,
                answer: "Done".to_owned()
            }
        )
        .await
        .is_err());
    let item = f.db.get_attention(&e.attention_id).await.unwrap().unwrap();
    assert_eq!(item.recommended_action, "answer_escalation");
    assert_eq!(
        AttentionService::new(
            f.db.clone(),
            Arc::new(services::WakeTurnConsumer::new(f.db.clone()))
        )
        .sweep_once_at(&now_rfc3339())
        .await
        .unwrap(),
        0,
        "owner escalation never wakes the blocked Agent"
    );
    service
        .answer(
            &f.project_id,
            &e.id,
            &f.account_id,
            api_types::AnswerProjectEscalationRequest {
                expected_version: 1,
                answer: "Disk space is now available".to_owned(),
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
    assert_eq!(turns(&f).await, 1);
    assert_eq!(
        charged(&f, "decision").await,
        0,
        "an owner answer is owner-initiated and never charged"
    );
    let content: String = sqlx::query_scalar(
        "SELECT content FROM agent_chat_message WHERE chat_id=? AND outcome='attention_wake'",
    )
    .bind(&f.chat_id)
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert!(content.contains("Disk space is now available"));
}
#[tokio::test]
async fn wake_audit_events_cannot_admit_a_second_turn() {
    let f = chat_turn_fixture().await;
    append_event(
        &f.db,
        CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "agent.wake.admitted".to_owned(),
            entity_type: "agent_wake".to_owned(),
            entity_id: "forged".to_owned(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: f.project_id.clone(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: None,
            payload_json:
                serde_json::json!({"identity_id":f.identity_id,"decision":"turn_admitted"})
                    .to_string(),
            created_at: now_rfc3339(),
        },
    )
    .await;
    WakeTurnConsumer::new(f.db.clone())
        .run_once(100)
        .await
        .unwrap();
    assert_eq!(turns(&f).await, 0);
}
#[tokio::test]
async fn unchanged_delivery_metadata_does_not_create_a_new_blocker_digest() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "execution_failed", "stable").await;
    let digest = wake_attention_incident_digest(&a);
    let mut updated = a;
    updated.version += 1;
    updated.source_event_id = new_uuid_v4();
    updated.source_sequence = Some(999);
    assert_eq!(digest, wake_attention_incident_digest(&updated));
    updated.details_json = serde_json::json!({"need":"different blocker"}).to_string();
    assert_ne!(digest, wake_attention_incident_digest(&updated));
    // n2: a renamed Task or a changed action offer is not a new blocker.
    let with = |title: &str, actions: serde_json::Value, reason: &str| {
        let mut a = updated.clone();
        a.details_json = serde_json::json!({
            "task": {"task_title": title, "task_status": "failed"},
            "recovery": {"requires_intervention": true, "actions": actions},
            "interruption": {"reason": reason},
        })
        .to_string();
        wake_attention_incident_digest(&a)
    };
    let base = with(
        "Build login",
        serde_json::json!([{"verb": "retry"}]),
        "tests failed",
    );
    assert_eq!(
        base,
        with(
            "Build the login page",
            serde_json::json!([]),
            "tests failed"
        )
    );
    assert_ne!(
        base,
        with(
            "Build login",
            serde_json::json!([{"verb": "retry"}]),
            "disk full"
        )
    );
}

#[tokio::test]
async fn unchanged_failed_delivery_is_suppressed_as_repeated_failure() {
    let f = chat_turn_fixture().await;
    let service = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    let a = incident(&f, "delivery_followup", "repeated").await;
    service
        .admit_wake(wake_request(&f, &a, &now_rfc3339()))
        .await
        .unwrap();
    AgentChatTurnWorker::with_runner(f.db.clone(), Arc::new(FailingWakeRunner))
        .run_once()
        .await
        .unwrap();
    let refreshed = incident(&f, "delivery_followup", "repeated").await;
    assert!(matches!(
        service
            .admit_wake(wake_request(&f, &refreshed, &now_rfc3339()))
            .await
            .unwrap(),
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::RepeatedFailure
        }
    ));
    assert_eq!(turns(&f).await, 1);
    assert_eq!(charged(&f, "delivery").await, 1);
}
#[tokio::test]
async fn turn_admission_failure_rolls_back_budget_lease_and_audit() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "delivery_followup", "atomic").await;
    sqlx::raw_sql("CREATE TRIGGER reject_turn BEFORE INSERT ON agent_chat_turn_job BEGIN SELECT RAISE(ABORT,'synthetic turn failure'); END;").execute(f.db.pool()).await.unwrap();
    assert!(AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone()))
    )
    .admit_wake(wake_request(&f, &a, &now_rfc3339()))
    .await
    .is_err());
    assert_eq!(turns(&f).await, 0);
    assert_eq!(charged(&f, "delivery").await, 0);
    let audit: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type='agent.wake.admitted'",
    )
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert_eq!(audit, 0);
}

#[tokio::test]
async fn zero_budget_sweep_does_not_repeat_the_owner_stall_notice() {
    let f = chat_turn_fixture().await;
    incident(&f, "execution_failed", "zero-budget").await;
    sqlx::query("UPDATE project_agent_binding SET wake_budget=0 WHERE project_id=?")
        .bind(&f.project_id)
        .execute(f.db.pool())
        .await
        .unwrap();
    let service = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    service.sweep_once_at(&now_rfc3339()).await.unwrap();
    service
        .sweep_once_at(&(chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339())
        .await
        .unwrap();
    let notices:i64=sqlx::query_scalar("SELECT COUNT(*) FROM domain_event WHERE event_type='notification.requested' AND scope_id=?").bind(&f.project_id).fetch_one(f.db.pool()).await.unwrap();
    assert_eq!(notices, 1);
    assert_eq!(turns(&f).await, 0);
    assert_eq!(charged(&f, "blocker").await, 0);
}

/// Ported from attention_service.rs (needs a ready Project chat now): a live
/// lease suppresses a duplicate, an expired lease inside the cooldown
/// suppresses as cooldown, a replay of the admitted source charges nothing,
/// a binding replacement cannot double-analyse a leased incident, and a
/// saturated bucket suppresses without another charge.
#[tokio::test]
async fn wake_policy_persists_cooldown_budget_and_global_identity_suppression() {
    let f = chat_turn_fixture().await;
    let service = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    let t0 = chrono::Utc::now();
    let at = |seconds: i64| (t0 + chrono::Duration::seconds(seconds)).to_rfc3339();
    let a = incident(&f, "delivery_followup", "cooldown").await;
    let mut first = wake_request(&f, &a, &at(0));
    first.lease_seconds = 30;
    first.cooldown_seconds = 60;
    assert!(matches!(
        service.admit_wake(first.clone()).await.unwrap(),
        services::WakeAdmissionResult::Admitted { .. }
    ));
    // Replaying the same source after the cooldown returns the original
    // admission without charging again.
    let mut replay = first.clone();
    replay.now = at(600);
    assert!(matches!(
        service.admit_wake(replay).await.unwrap(),
        services::WakeAdmissionResult::Admitted { .. }
    ));
    assert_eq!(charged(&f, "delivery").await, 1);
    // A new occurrence of the same incident while the lease is live.
    let refreshed = incident(&f, "delivery_followup", "cooldown").await;
    let mut duplicate = wake_request(&f, &refreshed, &at(10));
    duplicate.lease_seconds = 30;
    duplicate.cooldown_seconds = 60;
    assert!(matches!(
        service.admit_wake(duplicate.clone()).await.unwrap(),
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::DuplicateIncident
        }
    ));
    let mut cooling = duplicate;
    cooling.now = at(31);
    cooling.lease_owner = "replacement-worker".to_owned();
    assert!(matches!(
        service.admit_wake(cooling).await.unwrap(),
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::Cooldown
        }
    ));
    // A replacement binding cannot analyse an incident the old identity holds.
    let global = incident(&f, "delivery_followup", "global").await;
    assert!(matches!(
        service
            .admit_wake(wake_request(&f, &global, &at(0)))
            .await
            .unwrap(),
        services::WakeAdmissionResult::Admitted { .. }
    ));
    let replacement = new_uuid_v4();
    let replacement_profile =
        owned_identity_with_profile(&f.db, &replacement, &f.account_id, &new_uuid_v4()).await;
    sqlx::query("UPDATE project_agent_binding SET identity_id = ?, profile_id = ?, version = version + 1 WHERE project_id = ? AND state = 'active'")
        .bind(&replacement)
        .bind(&replacement_profile)
        .bind(&f.project_id)
        .execute(f.db.pool())
        .await
        .unwrap();
    let refreshed_global = incident(&f, "delivery_followup", "global").await;
    let mut other = wake_request(&f, &refreshed_global, &at(10));
    other.identity_id = replacement.clone();
    assert!(matches!(
        service.admit_wake(other).await.unwrap(),
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::DuplicateIncident
        }
    ));
    // A saturated bucket suppresses without counting the suppression.
    sqlx::query("UPDATE agent_wake_budget_window SET admitted_count = 4 WHERE scope_id = ? AND category = 'delivery'")
        .bind(&f.project_id)
        .execute(f.db.pool())
        .await
        .unwrap();
    let budget = incident(&f, "delivery_followup", "budget").await;
    let mut request = wake_request(&f, &budget, &at(30));
    request.identity_id = replacement;
    assert!(matches!(
        service.admit_wake(request).await.unwrap(),
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::BudgetExhausted
        }
    ));
    assert_eq!(charged(&f, "delivery").await, 4);
}

/// M3: a small budget is one shared pool, so no category is starved and no
/// false "stopped" notice is raised; the pool still bounds the hour.
#[tokio::test]
async fn small_wake_budget_shares_one_pool_across_categories() {
    let f = chat_turn_fixture().await;
    sqlx::query(
        "UPDATE project_agent_binding SET wake_budget=3 WHERE project_id=? AND state='active'",
    )
    .bind(&f.project_id)
    .execute(f.db.pool())
    .await
    .unwrap();
    let service = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    for (category, key) in [
        ("decision_recorded", "decision"),
        ("delivery_followup", "delivery-1"),
        ("delivery_followup", "delivery-2"),
    ] {
        let a = incident(&f, category, key).await;
        assert!(
            matches!(
                service
                    .admit_wake(wake_request(&f, &a, &now_rfc3339()))
                    .await
                    .unwrap(),
                services::WakeAdmissionResult::Admitted { .. }
            ),
            "{category} admitted from the shared pool"
        );
        audit35_finish_turns(&f).await;
    }
    let overflow = incident(&f, "decision_recorded", "overflow").await;
    assert!(matches!(
        service
            .admit_wake(wake_request(&f, &overflow, &now_rfc3339()))
            .await
            .unwrap(),
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::BudgetExhausted
        }
    ));
    assert_eq!(stall_notices(&f).await, 0);
    assert_eq!(turns(&f).await, 3);
}
