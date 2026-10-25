//! Legacy chat adoption, topic rotation recovery, and queued turn admission.

use super::services;
use super::*;

#[tokio::test]
async fn topic_rotation_recovers_after_mark_and_after_fork_before_commit() {
    use db::{AgentChatTopicRepo, AgentChatTopicTransactionRepo};
    use services::TopicRotator;
    let fixture = chat_fixture().await;
    let provider = scripted_provider(vec![
        text_step("Prior plan and unresolved work."),
        text_step("Keep the plan and unresolved work."),
    ]);
    let backend = Arc::new(
        NativeAgentRuntimeBackend::new(fixture.service.protected_store())
            .with_provider_override(provider.clone()),
    );
    backend
        .run_turn(
            fixture.turn("Remember the existing plan."),
            Arc::new(NoopSink),
        )
        .await
        .unwrap();
    let source_timeline: String = sqlx::query_scalar(
        "SELECT timeline_id FROM agent_runtime_lcm_binding WHERE runtime_session_id = ?",
    )
    .bind(&fixture.runtime_session_id)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    let service = Arc::new(fixture.service.with_native_backend(backend));
    let intent = new_uuid_v4();
    fixture
        .db
        .request_agent_chat_topic(db::RotateAgentChatTopic {
            runtime_session_id: None,
            rotation_owner: None,
            topic: db::CreateAgentChatTopic {
                id: intent.clone(),
                chat_id: fixture.scope.scope_id.clone(),
                label: "New topic".into(),
                summary: None,
                principal_type: "user".into(),
                principal_id: Some("user-1".into()),
                created_at: now_rfc3339(),
            },
            divider_message: db::topic_divider_message(
                new_uuid_v4(),
                fixture.scope.scope_id.clone(),
                "New topic",
                new_uuid_v4(),
                now_rfc3339(),
            ),
        })
        .await
        .unwrap();
    // Simulate restart after the mark, then fail the topic commit after fork.
    sqlx::query("CREATE TRIGGER fail_topic_commit BEFORE INSERT ON agent_chat_topic BEGIN SELECT RAISE(ABORT, 'injected crash after native fork'); END;")
        .execute(fixture.db.pool()).await.unwrap();
    let coordinator = services::TopicRotationCoordinator::new(fixture.db.clone(), service.clone());
    assert!(coordinator
        .rotate_pending(&fixture.scope.scope_id)
        .await
        .is_err());
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_topic_rotation WHERE id = ? AND summary_ciphertext IS NOT NULL")
        .bind(&intent).fetch_one(fixture.db.pool()).await.unwrap();
    assert_eq!(count, 1);
    sqlx::query("DROP TRIGGER fail_topic_commit")
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let calls = provider.requests().len();
    // The failed attempt backs off; the replay runs once it is due.
    assert!(
        services::TopicRotationCoordinator::new(fixture.db.clone(), service)
            .rotate_pending_at(
                &fixture.scope.scope_id,
                chrono::Utc::now() + chrono::Duration::seconds(10),
            )
            .await
            .unwrap()
    );
    assert_eq!(
        provider.requests().len(),
        calls,
        "replay reuses the protected summary seed"
    );
    assert_eq!(
        fixture
            .db
            .list_agent_chat_topics(&fixture.scope.scope_id)
            .await
            .unwrap()
            .len(),
        2
    );
    let successor: String =
        sqlx::query_scalar("SELECT runtime_session_id FROM agent_chat_topic WHERE id = ?")
            .bind(&intent)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    let next_timeline: String = sqlx::query_scalar(
        "SELECT timeline_id FROM agent_runtime_lcm_binding WHERE runtime_session_id = ?",
    )
    .bind(&successor)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    assert_ne!(source_timeline, next_timeline);
    let store = fixture
        .db
        .get_current_agent_chat_topic(&fixture.scope.scope_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(store.id, intent);
    let pending: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_topic_rotation WHERE chat_id = ?")
            .bind(&fixture.scope.scope_id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    assert_eq!(pending, 0);
}

#[tokio::test]
async fn pre_u7_resume_adopts_once_and_retired_timeline_still_loads() {
    let fixture = chat_fixture().await;
    let backend = NativeAgentRuntimeBackend::new(fixture.service.protected_store())
        .with_provider_override(text_only_provider(3));
    backend
        .run_turn(fixture.turn("First turn"), Arc::new(NoopSink))
        .await
        .unwrap();
    let timeline: String = sqlx::query_scalar(
        "SELECT timeline_id FROM agent_runtime_lcm_binding WHERE runtime_session_id = ?",
    )
    .bind(&fixture.runtime_session_id)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE agent_lcm_timeline SET claim_owner = NULL, claim_generation = 0, scope_id = canonical_scope_id || '#retired:' || id, retired_at = ? WHERE id = ?")
        .bind(now_rfc3339()).bind(&timeline).execute(fixture.db.pool()).await.unwrap();
    sqlx::query("DELETE FROM agent_runtime_lcm_binding WHERE runtime_session_id = ?")
        .bind(&fixture.runtime_session_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    backend
        .run_turn(fixture.turn("Resume after upgrade"), Arc::new(NoopSink))
        .await
        .unwrap();
    backend
        .run_turn(fixture.turn("Resume again"), Arc::new(NoopSink))
        .await
        .unwrap();
    let (owner, generation, retired): (String, i64, Option<String>) = sqlx::query_as(
        "SELECT claim_owner, claim_generation, retired_at FROM agent_lcm_timeline WHERE id = ?",
    )
    .bind(timeline)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    assert_eq!(owner, fixture.runtime_session_id);
    assert_eq!(generation, 1, "only the first upgraded resume adopts");
    assert!(retired.is_some(), "historical V149 data is preserved");
}

// ---- topic working set: upgrade, usage and rotation admission ----

/// Adopt claims generation 1 and then publishes the rebuilt session to the
/// journal. If that publication fails (crash, busy DB), the claim is durable
/// but the published state still carries generation 0. The same session
/// re-adopts idempotently, so the next turn resumes.
#[tokio::test]
async fn a_failed_save_after_the_adopt_claim_does_not_wedge_the_chat() {
    let fixture = chat_fixture().await;
    let backend = NativeAgentRuntimeBackend::new(fixture.service.protected_store())
        .with_provider_override(text_only_provider(4));
    backend
        .run_turn(fixture.turn("First turn"), Arc::new(NoopSink))
        .await
        .unwrap();
    let timeline: String = sqlx::query_scalar(
        "SELECT timeline_id FROM agent_runtime_lcm_binding WHERE runtime_session_id = ?",
    )
    .bind(&fixture.runtime_session_id)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    // Pre-U7 store shape: populated, unclaimed timeline, no binding row.
    sqlx::query(
        "UPDATE agent_lcm_timeline SET claim_owner = NULL, claim_generation = 0 WHERE id = ?",
    )
    .bind(&timeline)
    .execute(fixture.db.pool())
    .await
    .unwrap();
    sqlx::query("DELETE FROM agent_runtime_lcm_binding WHERE runtime_session_id = ?")
        .bind(&fixture.runtime_session_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let journal_heads = || async {
        sqlx::query_as::<_, (String, Vec<u8>)>(
            "SELECT role, revision FROM agent_session_journal_session
             WHERE forge_session_id = ? ORDER BY role",
        )
        .bind(&fixture.session_id)
        .fetch_all(fixture.db.pool())
        .await
        .unwrap()
    };
    let published = journal_heads().await;
    // Crash window: the first journal publication after Adopt's claim fails.
    // Resume publishes the adopted session to the ordinary domain before the
    // turn reaches its first protected checkpoint, so the ordinary engine's
    // next commit is exactly that write; the cut rolls it back.
    fixture
        .service
        .protected_store()
        .journals()
        .ordinary()
        .fail_next_commit_at(agent_runtime::core::journal::JournalCommitFailurePoint::AfterObjects)
        .expect("the publication cut arms");
    let crashed = backend
        .run_turn(fixture.turn("Upgrade turn"), Arc::new(NoopSink))
        .await;
    assert!(
        crashed.is_err(),
        "the injected publication failure fails this turn"
    );
    let (owner, generation): (Option<String>, i64) =
        sqlx::query_as("SELECT claim_owner, claim_generation FROM agent_lcm_timeline WHERE id = ?")
            .bind(&timeline)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    println!("adopt crash: after crash: owner={owner:?} generation={generation}");
    assert_eq!(
        (owner.as_deref(), generation),
        (Some(fixture.runtime_session_id.as_str()), 1),
        "Adopt's claim is durable although its publication failed"
    );
    assert_eq!(
        journal_heads().await,
        published,
        "neither journal domain published anything after the claim"
    );
    let first_retry = backend
        .run_turn(fixture.turn("Retry after restart"), Arc::new(NoopSink))
        .await;
    println!("adopt crash: retry 1: {:?}", first_retry.as_ref().err());
    let second_retry = backend
        .run_turn(fixture.turn("Retry again"), Arc::new(NoopSink))
        .await;
    println!("adopt crash: retry 2: {:?}", second_retry.as_ref().err());
    assert!(
        first_retry.is_ok() && second_retry.is_ok(),
        "a failed save after Adopt's claim wedges every later resume"
    );
}

/// A timeline written before any post-V149 turn has `runtime_session_id IS
/// NULL`. The upgrade turn adopts it (stamping its owner), and the first topic
/// rotation still forks onto a new timeline.
#[tokio::test]
async fn rotation_after_adopting_a_pre_v149_owner_less_timeline_forks_a_new_timeline() {
    use db::AgentChatTopicTransactionRepo;
    use services::TopicRotator;
    let fixture = chat_fixture().await;
    let provider = scripted_provider(vec![
        text_step("Prior plan."),
        text_step("Upgrade turn reply."),
        text_step("Topic summary."),
        text_step("After rotation."),
    ]);
    let backend = Arc::new(
        NativeAgentRuntimeBackend::new(fixture.service.protected_store())
            .with_provider_override(provider.clone()),
    );
    backend
        .run_turn(fixture.turn("Remember the plan."), Arc::new(NoopSink))
        .await
        .unwrap();
    let timeline: String = sqlx::query_scalar(
        "SELECT timeline_id FROM agent_runtime_lcm_binding WHERE runtime_session_id = ?",
    )
    .bind(&fixture.runtime_session_id)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    // Pre-V149-owner row: V149 backfilled canonical_scope_id but left runtime_session_id NULL.
    sqlx::query("UPDATE agent_lcm_timeline SET runtime_session_id = NULL, claim_owner = NULL, claim_generation = 0 WHERE id = ?")
        .bind(&timeline).execute(fixture.db.pool()).await.unwrap();
    sqlx::query("DELETE FROM agent_runtime_lcm_binding")
        .execute(fixture.db.pool())
        .await
        .unwrap();
    backend
        .run_turn(fixture.turn("First turn after upgrade"), Arc::new(NoopSink))
        .await
        .expect("upgrade turn adopts the legacy timeline");
    let service = Arc::new(fixture.service.with_native_backend(backend));
    fixture
        .db
        .request_agent_chat_topic(db::RotateAgentChatTopic {
            runtime_session_id: None,
            rotation_owner: None,
            topic: db::CreateAgentChatTopic {
                id: new_uuid_v4(),
                chat_id: fixture.scope.scope_id.clone(),
                label: "New topic".into(),
                summary: None,
                principal_type: "user".into(),
                principal_id: Some("user-1".into()),
                created_at: now_rfc3339(),
            },
            divider_message: db::topic_divider_message(
                new_uuid_v4(),
                fixture.scope.scope_id.clone(),
                "New topic",
                new_uuid_v4(),
                now_rfc3339(),
            ),
        })
        .await
        .unwrap();
    let coordinator = services::TopicRotationCoordinator::new(fixture.db.clone(), service);
    let first = coordinator.rotate_pending(&fixture.scope.scope_id).await;
    let bindings: Vec<(String, String)> =
        sqlx::query_as("SELECT runtime_session_id, timeline_id FROM agent_runtime_lcm_binding")
            .fetch_all(fixture.db.pool())
            .await
            .unwrap();
    println!("legacy rotation: first={first:?} bindings={bindings:?}");
    let second = coordinator.rotate_pending(&fixture.scope.scope_id).await;
    println!("legacy rotation: replay={second:?}");
    assert!(
        matches!(first, Ok(true)) || matches!(second, Ok(true)),
        "rotation of an adopted pre-V149 chat never completes"
    );
}

/// Through the real worker: a chat whose legacy timeline was adopted rotates
/// before its next turn, and that turn is then admitted rather than left
/// queued.
#[cfg(feature = "test-support")]
#[tokio::test]
async fn a_turn_queued_behind_a_legacy_chat_rotation_is_admitted() {
    use db::AgentChatTopicTransactionRepo;
    let chat = ledger_chat(
        MEASURED_CALLS
            .iter()
            .enumerate()
            .map(|(turn, call)| metered_text_step(&format!("reply {turn}"), *call))
            .collect(),
    )
    .await;
    assert_eq!(
        chat.turn("message 0").await.status,
        db::AgentChatTurnState::Succeeded
    );
    sqlx::query("UPDATE agent_lcm_timeline SET runtime_session_id = NULL, claim_owner = NULL, claim_generation = 0")
        .execute(chat.db.pool()).await.unwrap();
    sqlx::query("DELETE FROM agent_runtime_lcm_binding")
        .execute(chat.db.pool())
        .await
        .unwrap();
    assert_eq!(
        chat.turn("message 1").await.status,
        db::AgentChatTurnState::Succeeded
    );
    chat.db
        .request_agent_chat_topic(db::RotateAgentChatTopic {
            runtime_session_id: None,
            rotation_owner: None,
            topic: db::CreateAgentChatTopic {
                id: new_uuid_v4(),
                chat_id: chat.chat_id.clone(),
                label: "New topic".into(),
                summary: None,
                principal_type: "user".into(),
                principal_id: Some("user-1".into()),
                created_at: now_rfc3339(),
            },
            divider_message: db::topic_divider_message(
                new_uuid_v4(),
                chat.chat_id.clone(),
                "New topic",
                new_uuid_v4(),
                now_rfc3339(),
            ),
        })
        .await
        .unwrap();
    let admitted = chat
        .chats
        .send_message(services::SendAgentChatMessageInput {
            actor_user_id: "user-1".into(),
            chat_id: chat.chat_id.clone(),
            content: "message 2".into(),
            dedupe_key: Some("message 2".into()),
        })
        .await
        .unwrap();
    let mut claimed = 0;
    for _ in 0..5 {
        claimed += chat.worker.run_once().await.unwrap();
    }
    let job = chat.job(&admitted.turn_job.id).await;
    println!(
        "queued turn: claimed={claimed} status={:?} attempts={} error={:?}",
        job.status, job.attempt_count, job.error_code
    );
    assert_ne!(
        job.status,
        db::AgentChatTurnState::Queued,
        "the turn is neither run nor failed while the rotation cannot complete"
    );
}

/// Where a repeatedly failing rotation breaks.
#[cfg(feature = "test-support")]
#[derive(Clone, Copy, PartialEq, Eq)]
enum RotationBreak {
    /// The runtime fork cannot publish the successor's journal.
    Fork,
    /// The fork completes; Forge's topic commit fails.
    TopicCommit,
}

/// A rotation that keeps failing is bounded: three attempts with back-off,
/// then it is abandoned with a visible notice and a typed `rotation_failed`
/// event, the summary call is still charged once, and the turn that was
/// waiting runs. A failed fork keeps the chat on its current session; a fork
/// that completed hands the chat to the successor (the source is superseded).
#[cfg(feature = "test-support")]
async fn abandon_a_failing_rotation(fault: RotationBreak) {
    use db::{AgentChatTopicRepo, AgentChatTopicTransactionRepo};
    let chat = ledger_chat(
        MEASURED_CALLS[..3]
            .iter()
            .enumerate()
            .map(|(turn, call)| metered_text_step(&format!("reply {turn}"), *call))
            .collect(),
    )
    .await;
    assert_eq!(
        chat.turn("message 0").await.status,
        db::AgentChatTurnState::Succeeded
    );
    let topics_before = chat.db.list_agent_chat_topics(&chat.chat_id).await.unwrap();
    let intent = chat
        .db
        .request_agent_chat_topic(topic_request(&chat.chat_id, "Doomed topic"))
        .await
        .unwrap();
    let successor: String = sqlx::query_scalar(
        "SELECT successor_session_id FROM agent_chat_topic_rotation WHERE id = ?",
    )
    .bind(&intent)
    .fetch_one(chat.db.pool())
    .await
    .unwrap();
    match fault {
        RotationBreak::Fork => {
            // A storage fault scoped to the successor: every native journal
            // session write for it aborts inside the storage transaction, so
            // each fork attempt fails while the source keeps publishing.
            for event in ["INSERT", "UPDATE"] {
                sqlx::query(&format!("CREATE TRIGGER fail_successor_save_{event} BEFORE {event} ON agent_session_journal_session WHEN NEW.forge_session_id = '{successor}' BEGIN SELECT RAISE(ABORT, 'injected successor save failure'); END;"))
                    .execute(chat.db.pool()).await.unwrap();
            }
        }
        RotationBreak::TopicCommit => {
            sqlx::query("CREATE TRIGGER fail_topic_commit BEFORE INSERT ON agent_chat_topic BEGIN SELECT RAISE(ABORT, 'injected topic commit failure'); END;")
                .execute(chat.db.pool()).await.unwrap();
        }
    }
    // The chat's only session so far, whatever its idle status.
    let (original, original_status): (String, String) =
        sqlx::query_as("SELECT id, status FROM agent_session WHERE backend_kind = 'native'")
            .fetch_one(chat.db.pool())
            .await
            .unwrap();
    let admitted = chat
        .chats
        .send_message(services::SendAgentChatMessageInput {
            actor_user_id: "user-1".into(),
            chat_id: chat.chat_id.clone(),
            content: "message 1".into(),
            dedupe_key: Some("message 1".into()),
        })
        .await
        .unwrap();
    let start = chrono::Utc::now();
    let mut claimed = Vec::new();
    for offset in [0, 10, 60] {
        claimed.push(
            chat.worker
                .run_once_at(start + chrono::Duration::seconds(offset))
                .await
                .unwrap(),
        );
    }
    assert_eq!(
        claimed,
        vec![0, 0, 1],
        "the turn waits while attempts remain, then runs"
    );
    let job = chat.job(&admitted.turn_job.id).await;
    assert_eq!(
        job.status,
        db::AgentChatTurnState::Succeeded,
        "{:?}",
        job.error_message
    );
    let pending: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_topic_rotation")
        .fetch_one(chat.db.pool())
        .await
        .unwrap();
    assert_eq!(pending, 0, "the abandoned intent no longer blocks the chat");
    assert_eq!(
        chat.db.list_agent_chat_topics(&chat.chat_id).await.unwrap(),
        topics_before,
        "the chat keeps its current topic"
    );
    let statuses: (String, String) = sqlx::query_as(
        "SELECT (SELECT status FROM agent_session WHERE id = ?1),
                (SELECT status FROM agent_session WHERE id = ?2)",
    )
    .bind(&original)
    .bind(&successor)
    .fetch_one(chat.db.pool())
    .await
    .unwrap();
    let event: String = sqlx::query_scalar(
        "SELECT payload_json FROM domain_event WHERE event_type = 'agent_chat.topic.rotation_failed'",
    )
    .fetch_one(chat.db.pool())
    .await
    .unwrap();
    let event: serde_json::Value = serde_json::from_str(&event).unwrap();
    assert_eq!(event["attempts"], 3);
    let notice: (String, String) = sqlx::query_as(
        "SELECT author_type, content FROM agent_chat_message WHERE outcome = 'topic_rotation_failed'",
    )
    .fetch_one(chat.db.pool())
    .await
    .unwrap();
    assert_eq!(notice.0, "system");
    assert!(notice.1.contains("Doomed topic"));
    match fault {
        RotationBreak::Fork => {
            assert_eq!(statuses, (original_status, "failed".to_owned()));
            assert_eq!(event["error_kind"], "runtime");
            assert_eq!(event["session_handed_over"], false);
        }
        RotationBreak::TopicCommit => {
            assert_eq!(statuses.0, "replaced");
            assert_ne!(statuses.1, "failed");
            assert_eq!(event["error_kind"], "persistence");
            assert_eq!(event["session_handed_over"], true);
        }
    }
    let summary_calls = chat
        .provider
        .requests()
        .iter()
        .filter(|request| {
            request.messages.first().is_some_and(|message| {
                message
                    .joined_text()
                    .starts_with("Summarize the supplied conversation")
            })
        })
        .count();
    assert_eq!(summary_calls, 1, "replays reuse the sealed summary seed");
    let charged: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM usage_event WHERE input_tokens = ?")
            .bind(i64::try_from(MEASURED_CALLS[1].0).unwrap())
            .fetch_one(chat.db.pool())
            .await
            .unwrap();
    assert_eq!(
        charged, 1,
        "the summary call is charged once although the new topic never ran a turn"
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn a_rotation_whose_fork_keeps_failing_is_abandoned_and_the_queued_turn_runs() {
    abandon_a_failing_rotation(RotationBreak::Fork).await;
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn a_rotation_whose_topic_commit_keeps_failing_hands_the_chat_to_its_successor() {
    abandon_a_failing_rotation(RotationBreak::TopicCommit).await;
}

/// A provider summary failure alone never fails the rotation: the seed falls
/// back to the deterministic summary.
#[tokio::test]
async fn a_failed_topic_summary_falls_back_to_the_deterministic_seed() {
    use agent_runtime::core::provider::{ProviderError, ProviderErrorKind};
    use db::{AgentChatTopicRepo, AgentChatTopicTransactionRepo};
    use services::TopicRotator;
    let fixture = chat_fixture().await;
    let provider = scripted_provider(vec![
        text_step("The plan is agreed."),
        ScriptedStream::new(vec![ProviderStreamEvent::Error {
            error: ProviderError::new(ProviderErrorKind::BadRequest, "summary rejected"),
        }]),
    ]);
    let backend = Arc::new(
        NativeAgentRuntimeBackend::new(fixture.service.protected_store())
            .with_provider_override(provider.clone()),
    );
    backend
        .run_turn(fixture.turn("Agree the plan."), Arc::new(NoopSink))
        .await
        .unwrap();
    let service = Arc::new(fixture.service.with_native_backend(backend));
    let intent = fixture
        .db
        .request_agent_chat_topic(topic_request(&fixture.scope.scope_id, "Next topic"))
        .await
        .unwrap();
    assert!(
        services::TopicRotationCoordinator::new(fixture.db.clone(), service)
            .rotate_pending(&fixture.scope.scope_id)
            .await
            .expect("the rotation succeeds without a provider summary"),
    );
    let topic = fixture
        .db
        .get_current_agent_chat_topic(&fixture.scope.scope_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(topic.id, intent);
    assert_eq!(topic.label, "Next topic");
}
