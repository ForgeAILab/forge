//! Native provider rejection, authorization, and availability failures.

use super::*;

#[cfg(feature = "test-support")]
#[tokio::test]
async fn non_retryable_schema_rejection_fails_first_turn_attempt_with_one_provider_call() {
    native_rejection_with_one_provider_call(false).await;
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn provider_401_holds_second_queued_turn_on_the_same_credential() {
    native_rejection_with_one_provider_call(true).await;
}

#[cfg(feature = "test-support")]
async fn native_rejection_with_one_provider_call(auth_rejected: bool) {
    use agent_runtime::core::provider::{ProviderError, ProviderErrorKind};
    use db::AgentChatTurnJobRepo;
    let fixture = chat_fixture().await;
    let agent = main_agent_with_native_endpoint(&fixture).await;
    let chats = AgentChatService::new(fixture.db.clone());
    let admitted = chats
        .send_message(services::SendAgentChatMessageInput {
            actor_user_id: "user-1".into(),
            chat_id: fixture.scope.scope_id.clone(),
            content: "one provider request".into(),
            dedupe_key: Some("schema-rejection".into()),
        })
        .await
        .unwrap();
    let provider = scripted_provider(vec![ScriptedStream::new(vec![
        ProviderStreamEvent::Error {
            error: if auth_rejected {
                ProviderError::new(ProviderErrorKind::Auth, "provider returned HTTP 401")
            } else {
                ProviderError::new(ProviderErrorKind::BadRequest, "tool schema rejected")
            },
        },
    ])]);
    let backend = NativeAgentRuntimeBackend::new(fixture.service.protected_store())
        .with_provider_override(provider.clone());
    let logs = tempfile::tempdir().unwrap();
    let embedded = Arc::new(fixture.service.with_native_backend(Arc::new(backend)));
    let runner = services::FederatedAgentChatTurnRunner::new(
        fixture.db.clone(),
        embedded,
        Arc::new(UnreachableCli),
        services::AgentChatTurnLogRoot::new(logs.path()),
    );
    let worker = services::AgentChatTurnWorker::with_runner(fixture.db.clone(), Arc::new(runner));
    assert_eq!(worker.run_once().await.unwrap(), 1);
    let turn = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*fixture.db, &admitted.turn_job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(turn.status, db::AgentChatTurnState::Failed);
    assert_eq!(turn.attempt_count, 1);
    assert_eq!(
        turn.failure_class,
        Some(if auth_rejected {
            api_types::TurnFailure::ProviderAuth
        } else {
            api_types::TurnFailure::ProviderSchema
        })
    );
    assert_eq!(
        turn.retry_decision,
        Some(api_types::TurnRetryDecision::Fail)
    );
    if auth_rejected {
        let health = db::CredentialHandleRepo::get_provider_entry_health(
            &*fixture.db,
            agent.credential_ref.as_deref().unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(health.status, "error");
        assert_eq!(health.last_error_kind.as_deref(), Some("auth"));
        assert!(services::provider_health::is_unavailable(
            &health,
            chrono::Utc::now()
        ));
        let second = chats
            .send_message(services::SendAgentChatMessageInput {
                actor_user_id: "user-1".into(),
                chat_id: fixture.scope.scope_id.clone(),
                content: "queued behind the same credential".into(),
                dedupe_key: Some("after-auth-rejection".into()),
            })
            .await
            .unwrap();
        assert_eq!(worker.run_once().await.unwrap(), 0);
        let held = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*fixture.db, &second.turn_job.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(held.status, db::AgentChatTurnState::Queued);
        assert_eq!(held.attempt_count, 0);
    }
    assert_eq!(worker.run_once().await.unwrap(), 0);
    assert_eq!(provider.requests().len(), 1);
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn native_provider_availability_failures_are_configuration_before_provider_admission() {
    for condition in [
        "missing_model",
        "missing_credential",
        "disconnected",
        "disabled",
    ] {
        let fixture = chat_fixture().await;
        let binding =
            db::AccountMainAgentBindingRepo::get_active_main_binding(&*fixture.db, "user-1")
                .await
                .unwrap()
                .unwrap();
        let agent = AgentRepo::get_by_id(&*fixture.db, &binding.identity_id)
            .await
            .unwrap()
            .unwrap();
        let profile = db::AgentProfileRepo::get_profile(&*fixture.db, &agent.profile_id)
            .await
            .unwrap()
            .unwrap();
        let new_profile = new_uuid_v4();
        let now = now_rfc3339();
        db::AgentProfileRepo::create_and_select_profile(
            &*fixture.db,
            CreateAgentProfile {
                id: new_profile.clone(),
                identity_id: agent.id.clone(),
                backend_kind: profile.backend_kind,
                executor_type: profile.executor_type,
                provider: profile.provider,
                model: if condition == "missing_model" {
                    None
                } else {
                    profile.model
                },
                credential_ref: if condition == "missing_credential" {
                    None
                } else {
                    profile.credential_ref.clone()
                },
                reasoning_effort: profile.reasoning_effort,
                permission_policy: profile.permission_policy,
                prompt_template: profile.prompt_template,
                capabilities_json: profile.capabilities_json,
                tool_policy_json: profile.tool_policy_json,
                config_json: r#"{"base_url":"https://unused.invalid/v1"}"#.into(),
                daemon_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            db::SelectAgentProfile {
                identity_id: agent.id,
                profile_id: new_profile,
                expected_version: agent.version,
                updated_at: now,
            },
        )
        .await
        .unwrap();
        if condition == "disconnected" {
            sqlx::query("UPDATE credential_handle SET status = 'invalid' WHERE id = ?")
                .bind(profile.credential_ref.as_deref())
                .execute(fixture.db.pool())
                .await
                .unwrap();
        } else if condition == "disabled" {
            sqlx::query("UPDATE credential_handle SET enabled = 0 WHERE id = ?")
                .bind(profile.credential_ref.as_deref())
                .execute(fixture.db.pool())
                .await
                .unwrap();
        }
        // An already queued historical admission can reference incomplete
        // settings, and its provider entry can be disabled after admission.
        let message_id = new_uuid_v4();
        let turn_id = new_uuid_v4();
        sqlx::query("INSERT INTO agent_chat_message (id, chat_id, sequence, author_type, author_id, content, status, correlation_id, created_at) VALUES (?, ?, 1, 'user', 'user-1', 'configuration needs repair', 'complete', 'configuration-test', ?)")
            .bind(&message_id).bind(&fixture.scope.scope_id).bind(now_rfc3339()).execute(fixture.db.pool()).await.unwrap();
        let current = AgentRepo::get_by_id(&*fixture.db, &binding.identity_id)
            .await
            .unwrap()
            .unwrap();
        sqlx::query("INSERT INTO agent_chat_turn_job (id, chat_id, triggering_message_id, responder_identity_id, profile_id, canonical_scope_type, canonical_scope_id, dedupe_key, correlation_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'agent_chat', ?, ?, 'configuration-test', ?, ?)")
            .bind(&turn_id).bind(&fixture.scope.scope_id).bind(&message_id).bind(&current.id).bind(&current.profile_id)
            .bind(&fixture.scope.scope_id).bind(condition).bind(now_rfc3339()).bind(now_rfc3339()).execute(fixture.db.pool()).await.unwrap();
        let provider = scripted_provider(vec![]);
        let backend = NativeAgentRuntimeBackend::new(fixture.service.protected_store())
            .with_provider_override(provider.clone());
        let logs = tempfile::tempdir().unwrap();
        let runner = services::FederatedAgentChatTurnRunner::new(
            fixture.db.clone(),
            Arc::new(fixture.service.with_native_backend(Arc::new(backend))),
            Arc::new(UnreachableCli),
            services::AgentChatTurnLogRoot::new(logs.path()),
        );
        let worker =
            services::AgentChatTurnWorker::with_runner(fixture.db.clone(), Arc::new(runner));
        assert_eq!(worker.run_once().await.unwrap(), 1, "{condition}");
        let failed = db::AgentChatTurnJobRepo::get_agent_chat_turn_job(&*fixture.db, &turn_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failed.status, db::AgentChatTurnState::Failed, "{condition}");
        assert_eq!(
            failed.failure_class,
            Some(api_types::TurnFailure::Configuration),
            "{condition}"
        );
        assert!(provider.requests().is_empty(), "{condition}");
        let invocations: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM usage_invocation WHERE source_id = ?")
                .bind(&failed.id)
                .fetch_one(fixture.db.pool())
                .await
                .unwrap();
        assert_eq!(invocations, 0, "{condition}");
        let attention = services::AttentionService::new(
            fixture.db.clone(),
            Arc::new(services::WakeTurnConsumer::new(fixture.db.clone())),
        );
        attention.project_once(100).await.unwrap();
        attention.project_once(100).await.unwrap();
        let incidents: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM attention_projection WHERE details_json LIKE ?",
        )
        .bind(format!("%{}%", failed.id))
        .fetch_one(fixture.db.pool())
        .await
        .unwrap();
        assert_eq!(incidents, 1, "{condition}");
    }
}
