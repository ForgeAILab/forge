//! Wake turn consumer blocker recovery tests.

use super::*;

/// M1: an infrastructure failure never consumes the digest; the sweep
/// re-admits after the cooldown and nothing reaches the owner.
#[tokio::test]
async fn infrastructure_failed_blocker_turn_is_readmitted_after_cooldown() {
    let f = chat_turn_fixture().await;
    incident(&f, "execution_failed", "outage").await;
    let service = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    let worker = AgentChatTurnWorker::with_runner(f.db.clone(), Arc::new(TransientWakeRunner));
    for _ in 0..3 {
        sqlx::query("UPDATE agent_chat_turn_job SET next_attempt_at = NULL WHERE chat_id = ? AND status = 'retry_wait'")
            .bind(&f.chat_id)
            .execute(f.db.pool())
            .await
            .unwrap();
        worker.run_once().await.unwrap();
    }
    let status: String =
        sqlx::query_scalar("SELECT status FROM agent_chat_turn_job WHERE chat_id=?")
            .bind(&f.chat_id)
            .fetch_one(f.db.pool())
            .await
            .unwrap();
    assert_eq!(status, "failed");
    // Inside the Project's batch cooldown nothing is re-admitted.
    service.sweep_once_at(&now_rfc3339()).await.unwrap();
    assert_eq!(turns(&f).await, 1);
    let later = (chrono::Utc::now() + chrono::Duration::minutes(6)).to_rfc3339();
    assert_eq!(service.sweep_once_at(&later).await.unwrap(), 1);
    assert_eq!(
        (
            turns(&f).await,
            audit35_escalations(&f).await,
            stall_notices(&f).await
        ),
        (2, 0, 0)
    );
}

/// M1: a deterministic provider failure raises "can't run" once, never
/// escalates, and holds the blocker until the responder changes.
#[tokio::test]
async fn deterministic_blocker_failure_notifies_once_and_holds_until_the_profile_changes() {
    let f = chat_turn_fixture().await;
    incident(&f, "execution_failed", "auth").await;
    let service = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    // A usage limit with no reset hint holds for an hour.
    AgentChatTurnWorker::with_runner(f.db.clone(), Arc::new(FailingWakeRunner))
        .run_once()
        .await
        .unwrap();
    let title: String = sqlx::query_scalar(
        "SELECT json_extract(payload_json,'$.title') FROM domain_event
         WHERE event_type='notification.requested' AND scope_id=?",
    )
    .bind(&f.project_id)
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert_eq!(title, "Project Agent can't run: usage_limit");
    let soon = (chrono::Utc::now() + chrono::Duration::minutes(30)).to_rfc3339();
    service.sweep_once_at(&soon).await.unwrap();
    assert_eq!(
        (
            turns(&f).await,
            audit35_escalations(&f).await,
            stall_notices(&f).await
        ),
        (1, 0, 1),
        "held: no re-wake, no escalation, one notice"
    );
    // A new Profile re-arms the blocker immediately.
    select_profile(&f.db, &f.identity_id, &new_uuid_v4(), "repaired").await;
    assert_eq!(service.sweep_once_at(&soon).await.unwrap(), 1);
    assert_eq!(turns(&f).await, 2);
    assert_eq!(audit35_escalations(&f).await, 0);
}

/// M2: a turn whose lease keeps expiring is refunded three times, then each
/// expiry counts against max_attempts until the turn fails.
#[tokio::test]
async fn lease_expiry_refunds_are_capped_at_three_per_turn() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "delivery_followup", "crash-loop").await;
    AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    )
    .admit_wake(wake_request(&f, &a, &now_rfc3339()))
    .await
    .unwrap();
    let worker = AgentChatTurnWorker::with_runner(f.db.clone(), Arc::new(FailingWakeRunner));
    let mut seen = Vec::new();
    for _ in 0..6 {
        sqlx::query("UPDATE agent_chat_turn_job SET status='leased',attempt_count=attempt_count+1,invocation_count=invocation_count+1,lease_owner='crashed',leased_until='2000-01-01T00:00:00Z',next_attempt_at=NULL WHERE chat_id=? AND status IN ('queued','retry_wait')")
            .bind(&f.chat_id)
            .execute(f.db.pool())
            .await
            .unwrap();
        // A clock in the past recovers the lease but never re-claims the turn.
        worker
            .run_once_at(chrono::Utc::now() - chrono::Duration::days(1))
            .await
            .unwrap();
        let row: (String, i64, i64) = sqlx::query_as(
            "SELECT status, attempt_count, lease_refund_count FROM agent_chat_turn_job WHERE chat_id=?",
        )
        .bind(&f.chat_id)
        .fetch_one(f.db.pool())
        .await
        .unwrap();
        seen.push(row.clone());
        if row.0 == "failed" {
            break;
        }
    }
    assert_eq!(
        seen,
        vec![
            ("retry_wait".to_owned(), 0, 1),
            ("retry_wait".to_owned(), 0, 2),
            ("retry_wait".to_owned(), 0, 3),
            ("retry_wait".to_owned(), 1, 3),
            ("retry_wait".to_owned(), 2, 3),
            ("failed".to_owned(), 3, 3),
        ]
    );
}

/// M4: a generic Resolve on an escalation item is the owner's answer: it
/// closes the escalation, wakes the Agent uncharged, and re-arms the blocker.
#[tokio::test]
async fn resolving_an_escalation_answers_it_and_never_strands_the_blocker() {
    let f = chat_turn_fixture().await;
    incident(&f, "execution_failed", "needs-owner").await;
    let attention = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    assert_eq!(attention.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    let worker = AgentChatTurnWorker::with_runner(f.db.clone(), Arc::new(ProseOnlyWakeRunner));
    worker.run_once().await.unwrap();
    let escalations = services::project_escalation::ProjectEscalationService::new(f.db.clone());
    let open = escalations
        .list_for_owner(&f.project_id, &f.account_id, Some("open"), None, None)
        .await
        .unwrap();
    assert_eq!(open.items.len(), 1);
    let escalation = open.items[0].clone();
    assert!(escalation.need.starts_with("- Blocker needs-owner"));
    // A stranger cannot see it; the escalation is invisible to them.
    assert!(matches!(
        escalations
            .list_for_owner(&f.project_id, "stranger", None, None, None)
            .await,
        Err(ServiceError::NotFound { .. })
    ));
    answer_by_resolve(&f, &escalation).await;
    let answered = escalations
        .get_for_owner(&f.project_id, &escalation.id, &f.account_id)
        .await
        .unwrap();
    assert_eq!(
        (answered.status.as_str(), answered.answer.as_deref()),
        ("answered", Some("Resolved by the owner."))
    );
    assert!(escalations
        .list_for_owner(&f.project_id, &f.account_id, Some("open"), None, None)
        .await
        .unwrap()
        .items
        .is_empty());
    // The answer wakes the Agent without spending the decision bucket.
    attention.project_once(100).await.unwrap();
    assert_eq!(turns(&f).await, 2);
    assert_eq!(charged(&f, "decision").await, 0);
    // The blocker is linked to the answer turn; if it is still unchanged
    // after that turn, it escalates again rather than going silent.
    worker.run_once().await.unwrap();
    assert_eq!(audit35_escalations(&f).await, 2);
}

/// n1: a Project member who is not the owner gets 403, a stranger 404.
#[tokio::test]
async fn escalation_reads_are_owner_only() {
    let f = chat_turn_fixture().await;
    let member = new_uuid_v4();
    let now = now_rfc3339();
    UserRepo::create_user(
        &*f.db,
        &User {
            id: member.clone(),
            email: format!("{member}@example.test"),
            password_hash: "test".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO project_member (id, project_id, user_id, role, created_at, updated_at) VALUES (?, ?, ?, 'member', ?, ?)")
        .bind(new_uuid_v4()).bind(&f.project_id).bind(&member).bind(&now).bind(&now)
        .execute(f.db.pool()).await.unwrap();
    let service = services::project_escalation::ProjectEscalationService::new(f.db.clone());
    let e = service
        .escalate(
            &f.project_id,
            Agent(&f.identity_id),
            api_types::ProjectEscalateRequest {
                need: "Need a staging token".to_owned(),
                task_ids: vec![],
            },
            "token",
        )
        .await
        .unwrap();
    assert!(matches!(
        service.get_for_owner(&f.project_id, &e.id, &member).await,
        Err(ServiceError::AuthorizationDenied { .. })
    ));
    assert!(matches!(
        service
            .get_for_owner(&f.project_id, &e.id, "stranger")
            .await,
        Err(ServiceError::NotFound { .. })
    ));
    let page = service
        .list_for_owner(&f.project_id, &f.account_id, None, None, Some(1))
        .await
        .unwrap();
    assert_eq!((page.items.len(), page.has_more), (1, false));
}

/// M5: imported pre-upgrade admissions get one batched re-admission per
/// Project (80 blockers, 2 Projects: 2 wakes, 0 escalations); escalation
/// applies only after a post-upgrade turn for the same digest.
#[tokio::test]
async fn imported_blockers_get_one_batched_readmission_per_project() {
    let f = chat_turn_fixture().await;
    let second_identity = new_uuid_v4();
    let second_profile = identity_with_profile(&f.db, &second_identity).await;
    let (second_project, second_chat) =
        bound_project(&f.db, &second_identity, &second_profile).await;
    let legacy_turn = |chat_id: String, identity: String| {
        let db = f.db.clone();
        async move {
            let message_id = new_uuid_v4();
            let turn_id = new_uuid_v4();
            let now = now_rfc3339();
            sqlx::query("INSERT INTO agent_chat_message (id, chat_id, sequence, author_type, content, content_guard_json, sensitivity, status, source_type, source_metadata_json, correlation_id, created_at) VALUES (?, ?, (SELECT COALESCE(MAX(sequence),0)+1 FROM agent_chat_message WHERE chat_id = ?), 'system', 'legacy wake', '{}', 'internal', 'complete', 'native', '{}', ?, ?)")
                .bind(&message_id).bind(&chat_id).bind(&chat_id).bind(&message_id).bind(&now)
                .execute(db.pool()).await.unwrap();
            sqlx::query("INSERT INTO agent_chat_turn_job (id, chat_id, triggering_message_id, responder_identity_id, profile_id, canonical_scope_type, canonical_scope_id, status, dedupe_key, attempt_count, max_attempts, correlation_id, causation_depth, version, created_at, updated_at) VALUES (?, ?, ?, ?, (SELECT selected_profile_id FROM agent_identity WHERE id = ?), 'agent_chat', ?, 'succeeded', ?, 1, 3, ?, 0, 1, ?, ?)")
                .bind(&turn_id).bind(&chat_id).bind(&message_id).bind(&identity).bind(&identity)
                .bind(&chat_id).bind(format!("wake-turn:{turn_id}")).bind(&turn_id).bind(&now).bind(&now)
                .execute(db.pool()).await.unwrap();
            turn_id
        }
    };
    for (project_id, chat_id, identity) in [
        (
            f.project_id.clone(),
            f.chat_id.clone(),
            f.identity_id.clone(),
        ),
        (
            second_project.clone(),
            second_chat.clone(),
            second_identity.clone(),
        ),
    ] {
        let turn_id = legacy_turn(chat_id, identity).await;
        for n in 0..40 {
            let a = seed_incident(
                &f.db,
                &project_id,
                "execution_failed",
                &format!("attention:execution_failed:project:{project_id}:legacy:{n}"),
                &format!("Legacy blocker {n}"),
                serde_json::json!({"scope_type": "project", "scope_id": project_id, "n": n}),
                "inspect_run",
            )
            .await;
            // The shape V202610042145 imports: the pre-upgrade digest and source.
            sqlx::query("INSERT INTO agent_wake_blocker(attention_id,incident_digest,turn_job_id,admitted_at,legacy_source_event_id) VALUES(?,?,?,?,?)")
                .bind(&a.id).bind(format!("legacy-digest-{n}")).bind(&turn_id).bind(now_rfc3339()).bind(&a.source_event_id)
                .execute(f.db.pool()).await.unwrap();
        }
    }
    let service = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 2);
    let wakes: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'agent.wake.admitted'",
    )
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    let escalations: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_wake_escalation")
        .fetch_one(f.db.pool())
        .await
        .unwrap();
    assert_eq!((wakes, escalations), (2, 0));
    // After the post-upgrade turn leaves them unchanged, each Project's
    // owner gets one escalation covering its batch.
    sqlx::query("UPDATE agent_chat_turn_job SET status='succeeded' WHERE dedupe_key LIKE 'agent-wake-admitted:%'")
        .execute(f.db.pool())
        .await
        .unwrap();
    service
        .sweep_once_at(&(chrono::Utc::now() + chrono::Duration::minutes(10)).to_rfc3339())
        .await
        .unwrap();
    let per_project: Vec<(String, i64)> = sqlx::query_as(
        "SELECT project_id, COUNT(*) FROM agent_wake_escalation GROUP BY project_id ORDER BY project_id",
    )
    .fetch_all(f.db.pool())
    .await
    .unwrap();
    assert_eq!(per_project.len(), 2);
    assert!(per_project.iter().all(|(_, count)| *count == 1));
}

/// m1: a persisted Task transition by the turn's Agent is a recorded outcome;
/// the blocker escalates only if it recurs after that turn.
#[tokio::test]
async fn recorded_task_transition_prevents_escalation_until_the_blocker_recurs() {
    let f = chat_turn_fixture().await;
    let task_id = new_uuid_v4();
    let now = now_rfc3339();
    sqlx::query("INSERT INTO task (id, project_id, title, status, created_at, updated_at) VALUES (?, ?, 'retry me', 'failed', ?, ?)")
        .bind(&task_id).bind(&f.project_id).bind(&now).bind(&now)
        .execute(f.db.pool()).await.unwrap();
    let a = seed_incident(
        &f.db,
        &f.project_id,
        "review_risk",
        &format!(
            "attention:review_risk:project:{}:task:{task_id}",
            f.project_id
        ),
        "Review risk",
        serde_json::json!({"entity_type": "task", "entity_id": task_id}),
        "inspect_run",
    )
    .await;
    let service = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    let transition_id = new_uuid_v4();
    f.db.append_event(CreateDomainEvent::task_transition(
        transition_id.clone(),
        task_id.clone(),
        &f.project_id,
        "failed",
        "todo",
        None,
        format!("agent:{}", f.identity_id),
        "retry after fixing the cause",
        false,
        now_rfc3339(),
        serde_json::Value::Null,
    ))
    .await
    .unwrap();
    audit35_finish_turns(&f).await;
    service
        .sweep_once_at(&(chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339())
        .await
        .unwrap();
    assert_eq!(
        audit35_escalations(&f).await,
        0,
        "the recovery was recorded"
    );
    // The same blocker recurs after the turn: the recovery did not take.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let recurred = seed_incident(
        &f.db,
        &f.project_id,
        "review_risk",
        &a.dedupe_key,
        "Review risk",
        serde_json::json!({"entity_type": "task", "entity_id": task_id}),
        "inspect_run",
    )
    .await;
    assert_eq!(recurred.id, a.id);
    service
        .sweep_once_at(&(chrono::Utc::now() + chrono::Duration::minutes(2)).to_rfc3339())
        .await
        .unwrap();
    assert_eq!(audit35_escalations(&f).await, 1);
}

/// m7: the sweep reads open Attention plus one keyed decision row; it never
/// scans the disposition history, however large.
#[tokio::test]
async fn sweep_candidate_query_does_not_scan_dispositions() {
    let f = chat_turn_fixture().await;
    let plan: Vec<(i64, i64, i64, String)> = sqlx::query_as(
        "EXPLAIN QUERY PLAN
         SELECT a.id, a.scope_type, a.scope_id, l.incident_digest, l.disposition, l.reason, l.identity_id
         FROM attention_projection a
         LEFT JOIN agent_wake_attention_latest l ON l.attention_id = a.id
         WHERE a.status = 'open' AND a.recommended_action <> 'answer_escalation'
           AND (a.snoozed_until IS NULL OR a.snoozed_until <= ?)
         ORDER BY a.scope_type, a.scope_id, a.occurred_at, a.id",
    )
    .bind(now_rfc3339())
    .fetch_all(f.db.pool())
    .await
    .unwrap();
    let detail = plan
        .iter()
        .map(|row| row.3.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!detail.contains("agent_wake_disposition"), "{detail}");
    assert!(!detail.contains("SCAN l"), "{detail}");
    assert!(
        detail.contains("idx_attention_projection_status_priority"),
        "{detail}"
    );
    let blocker: Vec<(i64, i64, i64, String)> = sqlx::query_as(
        "EXPLAIN QUERY PLAN SELECT j.status FROM agent_wake_blocker b
         JOIN agent_chat_turn_job j ON j.id = b.turn_job_id WHERE b.attention_id = ?",
    )
    .bind("x")
    .fetch_all(f.db.pool())
    .await
    .unwrap();
    let detail = blocker
        .iter()
        .map(|row| row.3.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!detail.contains("SCAN"), "{detail}");
}

/// B1: a resolve followed by a reopen re-arms the blocker, even with the same
/// digest and even after its turn was escalated.
#[tokio::test]
async fn resolved_then_reopened_blocker_is_woken_again() {
    let f = chat_turn_fixture().await;
    let a = incident(&f, "execution_failed", "flaky").await;
    let service = AttentionService::new(
        f.db.clone(),
        Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
    );
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    AgentChatTurnWorker::with_runner(f.db.clone(), Arc::new(ProseOnlyWakeRunner))
        .run_once()
        .await
        .unwrap();
    assert_eq!(
        audit35_escalations(&f).await,
        1,
        "the silent turn escalated"
    );
    let current = f.db.get_attention(&a.id).await.unwrap().unwrap();
    service
        .resolve(&f.account_id, &a.id, current.version)
        .await
        .unwrap();
    let reopened = incident(&f, "execution_failed", "flaky").await;
    assert_eq!(
        (reopened.id.as_str(), reopened.status.as_str()),
        (a.id.as_str(), "open")
    );
    assert_eq!(
        wake_attention_incident_digest(&reopened),
        wake_attention_incident_digest(&a)
    );
    let later = (chrono::Utc::now() + chrono::Duration::minutes(6)).to_rfc3339();
    assert_eq!(service.sweep_once_at(&later).await.unwrap(), 1);
    assert_eq!((turns(&f).await, audit35_escalations(&f).await), (2, 1));
}

/// Root lead, stage 2: the one lead filter of the blocker sweep. A subtask's
/// blocker under a coordination root with a lead wakes nobody while the lead
/// is about to re-plan for it; once the lead re-planned and left the park in
/// place, the same open blocker wakes the Project Agent on the next sweep.
/// Without a lead it wakes the Project Agent at once, as before.
#[tokio::test]
async fn a_subtask_blocker_under_a_lead_wakes_the_project_agent_only_after_the_replan() {
    for with_lead in [true, false] {
        let f = chat_turn_fixture().await;
        let now = now_rfc3339();
        let (root, child) = (new_uuid_v4(), new_uuid_v4());
        for (id, parent, status) in [
            (&root, None, "in_progress"),
            (&child, Some(&root), "in_progress"),
        ] {
            sqlx::query(
                "INSERT INTO task (id, project_id, parent_task_id, title, description, status, priority, subtask_order, created_at, updated_at)
                 VALUES (?, ?, ?, 'Step', 'fixture', ?, 0, 0, ?, ?)",
            )
            .bind(id)
            .bind(&f.project_id)
            .bind(parent)
            .bind(status)
            .bind(&now)
            .bind(&now)
            .execute(f.db.pool())
            .await
            .unwrap();
        }
        let parked_at = "2026-10-12T01:00:00+00:00";
        sqlx::query("UPDATE task SET blocked_json = ?, error_annotation = ? WHERE id = ?")
            .bind(
                serde_json::json!({"reason": "retries exhausted", "created_at": parked_at, "kind": "retry_exhausted"})
                    .to_string(),
            )
            .bind(
                serde_json::json!({"type": "retry_exhausted", "blocking_reason": "retries exhausted", "message": "retries exhausted", "blocked_at": parked_at})
                    .to_string(),
            )
            .bind(&child)
            .execute(f.db.pool())
            .await
            .unwrap();
        if with_lead {
            sqlx::query(
                "INSERT INTO task_role_assignment (id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at)
                 VALUES (?, ?, 'planner', 'agent', ?, ?, ?)",
            )
            .bind(new_uuid_v4())
            .bind(&root)
            .bind(&f.identity_id)
            .bind(&now)
            .bind(&now)
            .execute(f.db.pool())
            .await
            .unwrap();
        }
        let blocker = incident(&f, "execution_failed", "subtask-parked").await;
        sqlx::query("UPDATE attention_projection SET details_json = ? WHERE id = ?")
            .bind(
                serde_json::json!({"scope_type": "project", "scope_id": f.project_id, "entity_type": "task", "entity_id": child})
                    .to_string(),
            )
            .bind(&blocker.id)
            .execute(f.db.pool())
            .await
            .unwrap();
        let service = AttentionService::new(
            f.db.clone(),
            Arc::new(services::WakeTurnConsumer::new(f.db.clone())),
        );
        let admitted = service.sweep_once_at(&now_rfc3339()).await.unwrap();
        if !with_lead {
            assert_eq!(
                (admitted, turns(&f).await),
                (1, 1),
                "no lead: woken as before"
            );
            continue;
        }
        assert_eq!(
            (admitted, turns(&f).await),
            (0, 0),
            "the lead is about to re-plan for this park: nobody is woken"
        );
        // The lead re-planned for this park and left it in place: the root
        // implements again and the park is the Project Agent's.
        db::TransitionLogRepo::insert(
            &*f.db,
            db::CreateTransitionLog {
                id: new_uuid_v4(),
                task_id: root.clone(),
                from_state: "in_progress".to_owned(),
                to_state: "planning".to_owned(),
                trigger_name: None,
                triggered_by: "system:task_dispatcher".to_owned(),
                bridge: api_types::TransitionBridge {
                    bridge_kind: Some(api_types::TransitionBridgeKind::Replan),
                    bridge_payload: Some(serde_json::json!({
                        "trigger": "subtask_parked",
                        "subject_id": child,
                        "receipt": format!("replan:{child}:{parked_at}"),
                    })),
                },
                trigger_reason: "subtask parked".to_owned(),
                hook_results_json: None,
                rejection: false,
                created_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
        let later = (chrono::Utc::now() + chrono::Duration::minutes(10)).to_rfc3339();
        assert_eq!(
            (
                service.sweep_once_at(&later).await.unwrap(),
                turns(&f).await
            ),
            (1, 1),
            "the park the lead left in place wakes the Project Agent"
        );
    }
}
