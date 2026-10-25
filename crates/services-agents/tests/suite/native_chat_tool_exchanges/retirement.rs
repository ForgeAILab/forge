//! Retired native sessions and bounded retirement races.

use super::*;

async fn resume_retirement_fixture(fixture: &ChatFixture, profile_id: &str) -> db::AgentSession {
    use db::AgentSessionRepo;
    let row = fixture
        .db
        .get_agent_session(&fixture.session_id)
        .await
        .unwrap()
        .unwrap();
    fixture
        .service
        .create_or_resume_session(CreateScopedSession {
            actor_user_id: "user-1".into(),
            identity_id: row.identity_id,
            profile_id: Some(profile_id.into()),
            scope: RequestedCanonicalScope::AgentChat {
                chat_id: fixture.scope.scope_id.clone(),
            },
        })
        .await
        .unwrap()
}

fn retirement_turn(
    fixture: &ChatFixture,
    session: &db::AgentSession,
    input: &str,
) -> AgentTurnRequest {
    let mut request = fixture.turn(input);
    request.forge_session_id = session.id.clone();
    request.runtime_session_id = session.runtime_session_id.clone().unwrap();
    request
}

#[tokio::test]
async fn retired_older_dormant_session_rotates_after_newer_profile_replacement() {
    use db::{AgentProfileRepo, AgentSessionRepo};
    use services_agents::session_journal_retention::SessionJournalRetention;
    let mut fixture = chat_fixture().await;
    let store = fixture.service.protected_store();
    let provider = text_only_provider(8);
    let backend = Arc::new(
        NativeAgentRuntimeBackend::new(store.clone()).with_provider_override(provider.clone()),
    );
    fixture.service = fixture.service.with_native_backend(backend.clone());
    backend
        .run_turn(fixture.turn("A history"), Arc::new(NoopSink))
        .await
        .unwrap();
    let a = fixture
        .db
        .get_agent_session(&fixture.session_id)
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE agent_session SET status='suspended',created_at='2026-01-01',version=version+1 WHERE id=?")
        .bind(&a.id).execute(fixture.db.pool()).await.unwrap();
    let b = fixture
        .db
        .create_agent_session(db::CreateAgentSession {
            id: new_uuid_v4(),
            identity_id: a.identity_id.clone(),
            profile_id: a.profile_id.clone(),
            context_scope_id: a.context_scope_id.clone(),
            backend_kind: "native".into(),
            runtime_session_id: Some(new_uuid_v4()),
            status: "ready".into(),
            capabilities_json: a.capabilities_json.clone(),
            connection_status: a.connection_status.clone(),
            predecessor_session_id: None,
            last_activity_at: None,
            created_at: "2026-02-01".into(),
            updated_at: now_rfc3339(),
        })
        .await
        .unwrap();
    backend
        .run_turn(
            retirement_turn(&fixture, &b, "B history"),
            Arc::new(NoopSink),
        )
        .await
        .unwrap();
    fixture
        .db
        .suspend_stale_native_sessions(&now_rfc3339())
        .await
        .unwrap();
    let retention = SessionJournalRetention::new(fixture.db.clone(), &store);
    let retired = retention.retire_page("").await.unwrap();
    assert_eq!(retired.retired_domains, 2);
    assert!(
        fixture
            .db
            .session_persistence_flags(&a.id)
            .await
            .unwrap()
            .retired
    );
    assert!(
        !fixture
            .db
            .session_persistence_flags(&b.id)
            .await
            .unwrap()
            .retired
    );
    assert_eq!(
        resume_retirement_fixture(&fixture, &a.profile_id).await.id,
        b.id
    );

    let p = fixture
        .db
        .get_profile(&a.profile_id)
        .await
        .unwrap()
        .unwrap();
    let q = new_uuid_v4();
    fixture
        .db
        .create_profile(db::CreateAgentProfile {
            id: q.clone(),
            identity_id: p.identity_id,
            backend_kind: p.backend_kind,
            executor_type: p.executor_type,
            provider: p.provider,
            model: p.model,
            reasoning_effort: p.reasoning_effort,
            permission_policy: p.permission_policy,
            prompt_template: p.prompt_template,
            capabilities_json: p.capabilities_json,
            tool_policy_json: p.tool_policy_json,
            config_json: p.config_json,
            credential_ref: p.credential_ref,
            daemon_id: p.daemon_id,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        })
        .await
        .unwrap();
    let q_session = resume_retirement_fixture(&fixture, &q).await;
    assert_eq!(
        q_session.predecessor_session_id.as_deref(),
        Some(b.id.as_str())
    );
    assert_eq!(
        fixture
            .db
            .suspend_stale_native_sessions(&now_rfc3339())
            .await
            .unwrap(),
        1
    );
    // A restart has cold journal engines as well as a fresh per-turn Runtime.
    drop(retention);
    drop(backend);
    drop(store);
    let restarted = EmbeddedAgentService::new(fixture.db.clone(), b"tool-call-id-reuse-test-key");
    let backend = Arc::new(
        NativeAgentRuntimeBackend::new(restarted.protected_store())
            .with_provider_override(provider.clone()),
    );
    fixture.service = restarted.with_native_backend(backend);
    let fresh = resume_retirement_fixture(&fixture, &a.profile_id).await;
    assert_eq!(fresh.predecessor_session_id.as_deref(), Some(a.id.as_str()));
    assert_ne!(fresh.runtime_session_id, a.runtime_session_id);
    assert_eq!(fresh.profile_id, a.profile_id);
    assert_eq!(fresh.context_scope_id, a.context_scope_id);
    assert_eq!(fresh.identity_id, a.identity_id);
    for input in ["Fresh turn", "Second turn"] {
        let selected = resume_retirement_fixture(&fixture, &a.profile_id).await;
        assert_eq!(selected.id, fresh.id);
        let output = fixture
            .service
            .run_native_turn(
                &mut retirement_turn(&fixture, &selected, input),
                Arc::new(NoopSink),
            )
            .await
            .unwrap();
        assert_eq!(
            output.runtime_session_id,
            fresh.runtime_session_id.clone().unwrap()
        );
    }
    assert!(
        fixture
            .db
            .session_persistence_flags(&a.id)
            .await
            .unwrap()
            .retired,
        "tombstone remains"
    );
    let texts = user_texts(provider.requests().last().unwrap());
    assert!(!texts
        .iter()
        .any(|text| text == "A history" || text == "B history"));
    assert_eq!(texts.last().map(String::as_str), Some("Second turn"));
}

#[tokio::test]
async fn retirement_between_selection_and_open_rotates_once_and_retries() {
    use db::AgentSessionRepo;
    let mut fixture = chat_fixture().await;
    let store = fixture.service.protected_store();
    let backend = Arc::new(
        NativeAgentRuntimeBackend::new(store.clone()).with_provider_override(text_only_provider(5)),
    );
    fixture.service = fixture.service.with_native_backend(backend.clone());
    backend
        .run_turn(fixture.turn("Published turn"), Arc::new(NoopSink))
        .await
        .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    backend.set_open_barrier_for_test(barrier.clone());
    let mut request = fixture.turn("Racing turn");
    let (result, ()) = tokio::join!(
        fixture
            .service
            .run_native_turn(&mut request, Arc::new(NoopSink)),
        async {
            barrier.wait().await;
            let progress = store
                .journals()
                .retire_published(&fixture.runtime_session_id)
                .await
                .unwrap();
            assert_eq!(progress.retired_domains, 2);
            barrier.wait().await;
        }
    );
    let output = result.expect("retired open rotates and retries once");
    assert_ne!(output.runtime_session_id, fixture.runtime_session_id);
    let original = fixture
        .db
        .get_agent_session(&fixture.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.status, "replaced");
    let rows = fixture
        .db
        .list_agent_sessions(&original.identity_id)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "exactly one host rotation");
    let fresh = fixture
        .db
        .get_agent_session(original.replaced_by_session_id.as_deref().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        fresh.runtime_session_id.as_deref(),
        Some(output.runtime_session_id.as_str())
    );
    fixture
        .service
        .run_native_turn(
            &mut retirement_turn(&fixture, &fresh, "Next turn"),
            Arc::new(NoopSink),
        )
        .await
        .unwrap();
    assert!(
        fixture
            .db
            .session_persistence_flags(&original.id)
            .await
            .unwrap()
            .retired
    );
}

#[tokio::test]
async fn repeated_retired_open_races_do_not_retain_topic_read_markers() {
    use db::AgentSessionRepo;
    let mut fixture = chat_fixture().await;
    let store = fixture.service.protected_store();
    let backend = Arc::new(
        NativeAgentRuntimeBackend::new(store.clone())
            .with_provider_override(text_only_provider(17)),
    );
    fixture.service = fixture.service.with_native_backend(backend.clone());
    let mut request = fixture.turn("Publish initial history");
    fixture
        .service
        .run_native_turn(&mut request, Arc::new(NoopSink))
        .await
        .unwrap();
    for race in 0..8 {
        let retired_runtime = request.runtime_session_id.clone();
        let retired_host = request.forge_session_id.clone();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        backend.set_open_barrier_for_test(barrier.clone());
        request.input = format!("Racing turn {race}");
        let (result, ()) = tokio::join!(
            fixture
                .service
                .run_native_turn(&mut request, Arc::new(NoopSink)),
            async {
                barrier.wait().await;
                assert_eq!(
                    store
                        .journals()
                        .retire_published(&retired_runtime)
                        .await
                        .unwrap()
                        .retired_domains,
                    2
                );
                barrier.wait().await;
            }
        );
        let output = result.unwrap();
        assert_ne!(output.runtime_session_id, retired_runtime);
        assert_eq!(output.runtime_session_id, request.runtime_session_id);
        assert_eq!(
            fixture
                .db
                .get_agent_session(&retired_host)
                .await
                .unwrap()
                .unwrap()
                .status,
            "replaced"
        );
        assert_eq!(
            backend.failed_topic_read_count_for_test(),
            0,
            "retired-open race {race} must not retain an obsolete runtime id"
        );
    }
}

#[tokio::test]
async fn retired_active_session_rotates_before_reuse() {
    use db::AgentSessionRepo;
    let mut fixture = chat_fixture().await;
    let store = fixture.service.protected_store();
    let backend = Arc::new(
        NativeAgentRuntimeBackend::new(store.clone()).with_provider_override(text_only_provider(4)),
    );
    fixture.service = fixture.service.with_native_backend(backend.clone());
    backend
        .run_turn(fixture.turn("Published turn"), Arc::new(NoopSink))
        .await
        .unwrap();
    assert_eq!(
        store
            .journals()
            .retire_published(&fixture.runtime_session_id)
            .await
            .unwrap()
            .retired_domains,
        2
    );
    let mut mismatched = fixture.turn("Wrong scope");
    mismatched.scope.scope_id = new_uuid_v4();
    assert!(matches!(
        fixture
            .service
            .run_native_turn(&mut mismatched, Arc::new(NoopSink))
            .await,
        Err(forge_agent_host::AgentHostError::Authority(_))
    ));
    let previous = fixture
        .db
        .get_agent_session(&fixture.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        fixture
            .db
            .list_agent_sessions(&previous.identity_id)
            .await
            .unwrap()
            .len(),
        1,
        "binding refusal must precede retirement rotation"
    );
    assert!(matches!(
        backend
            .run_turn(fixture.turn("Retired id"), Arc::new(NoopSink))
            .await,
        Err(forge_agent_host::AgentHostError::SessionRetired)
    ));
    let previous = fixture
        .db
        .get_agent_session(&fixture.session_id)
        .await
        .unwrap()
        .unwrap();
    let fresh = resume_retirement_fixture(&fixture, &previous.profile_id).await;
    assert_eq!(
        fresh.predecessor_session_id.as_deref(),
        Some(previous.id.as_str())
    );
    assert_ne!(fresh.runtime_session_id, previous.runtime_session_id);
    fixture
        .service
        .run_native_turn(
            &mut retirement_turn(&fixture, &fresh, "Fresh turn"),
            Arc::new(NoopSink),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn second_retirement_during_open_is_a_bounded_turn_failure() {
    use db::AgentSessionRepo;
    let mut fixture = chat_fixture().await;
    let store = fixture.service.protected_store();
    let backend = Arc::new(
        NativeAgentRuntimeBackend::new(store.clone()).with_provider_override(text_only_provider(4)),
    );
    fixture.service = fixture.service.with_native_backend(backend.clone());
    backend
        .run_turn(fixture.turn("Published turn"), Arc::new(NoopSink))
        .await
        .unwrap();
    let first = Arc::new(tokio::sync::Barrier::new(2));
    let second = Arc::new(tokio::sync::Barrier::new(2));
    backend.set_open_barrier_for_test(first.clone());
    let mut request = fixture.turn("Twice retired");
    let (result, ()) = tokio::join!(
        fixture
            .service
            .run_native_turn(&mut request, Arc::new(NoopSink)),
        async {
            first.wait().await;
            assert_eq!(
                store
                    .journals()
                    .retire_published(&fixture.runtime_session_id)
                    .await
                    .unwrap()
                    .retired_domains,
                2
            );
            backend.set_open_barrier_for_test(second.clone());
            first.wait().await;
            second.wait().await;
            let previous = fixture
                .db
                .get_agent_session(&fixture.session_id)
                .await
                .unwrap()
                .unwrap();
            let fresh = fixture
                .db
                .get_agent_session(previous.replaced_by_session_id.as_deref().unwrap())
                .await
                .unwrap()
                .unwrap();
            // Another request publishes then retires the retry's identity before
            // its native open. The retry must not allocate a third host row.
            let publisher = NativeAgentRuntimeBackend::new(store.clone())
                .with_provider_override(text_only_provider(1));
            publisher
                .run_turn(
                    retirement_turn(&fixture, &fresh, "Concurrent publication"),
                    Arc::new(NoopSink),
                )
                .await
                .unwrap();
            assert_eq!(
                store
                    .journals()
                    .retire_published(fresh.runtime_session_id.as_deref().unwrap())
                    .await
                    .unwrap()
                    .retired_domains,
                2
            );
            second.wait().await;
        }
    );
    assert!(
        matches!(
            result,
            Err(forge_agent_host::AgentHostError::SessionRetired)
        ),
        "{result:?}"
    );
    let previous = fixture
        .db
        .get_agent_session(&fixture.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        fixture
            .db
            .list_agent_sessions(&previous.identity_id)
            .await
            .unwrap()
            .len(),
        2
    );
}
