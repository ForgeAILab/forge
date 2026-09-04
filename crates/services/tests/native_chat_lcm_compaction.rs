//! Proves the embedded (native) Agent Chat path engages LCM for a Main Agent
//! chat scope end to end: the chat-scoped timeline is created, canonical
//! history is admitted as immutable entries, hard context pressure condenses
//! history into summary nodes before the provider call, the turn's manifest
//! links the timeline, and the chat keeps working after compaction.
//!
//! The transport's SSRF policy correctly rejects loopback endpoints, so the
//! turn runs against the real `NativeAgentRuntimeBackend::run_turn` with an
//! in-process scripted runtime provider instead of a mock HTTP server. LCM is
//! an embedded-agent capability only; the CLI chat backend advertises
//! `lcm: false` and is intentionally out of scope here.

use std::sync::Arc;

use agent_runtime::core::provider::{Capabilities, FinishReason, ProviderStreamEvent};
use agent_runtime::provider::fake::{usage_event, FakeProvider, ScriptedStream};
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentRepo, AgentStatus,
    CreateAgentIdentity, CreateAgentProfile, SqliteDb,
};
use forge_agent_host::{
    AgentSessionBackend, AgentTurnRequest, CanonicalScope, CanonicalScopeType, Message,
    NativeAgentRuntimeBackend, NativeProviderConfig, Role, Secret, TurnEventSink, WorkspaceAccess,
};
use services::embedded_agent_service::{CreateScopedSession, RequestedCanonicalScope};
use services::{AgentChatService, EmbeddedAgentService, SetMainAgentBindingInput};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
struct NoopSink;

#[async_trait::async_trait]
impl TurnEventSink for NoopSink {}

async fn sqlite_db() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES ('user-1', 'user-1@example.test', 'test', NULL, ?, ?)",
    )
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("user creates");
    db
}

async fn native_identity(db: &SqliteDb, credential_id: &str) -> (String, String) {
    let identity_id = new_uuid_v4();
    let profile_id = new_uuid_v4();
    let now = now_rfc3339();
    AgentRepo::create_identity_with_profile(
        db,
        CreateAgentIdentity {
            id: identity_id.clone(),
            name: "embedded-main-agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some("user-1".to_owned()),
            visibility: "account".to_owned(),
            // A lean ceiling keeps the composed tool surface small so the
            // un-compactable floor stays well under the input budget and the
            // test observes condensation rather than a cannot-fit refusal.
            account_permission_ceiling: serde_json::json!({
                "permissions": ["read_agent_chat", "read_memory"]
            })
            .to_string(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: profile_id.clone(),
            identity_id: identity_id.clone(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("openai".to_owned()),
            model: Some("fake".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: serde_json::json!({
                "allowed": ["read_agent_chat", "read_memory"]
            })
            .to_string(),
            config_json: "{}".to_owned(),
            credential_ref: Some(credential_id.to_owned()),
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("identity creates");
    (identity_id, profile_id)
}

async fn inquiry_fixture(
    scripts: Vec<ScriptedStream>,
) -> (
    Arc<SqliteDb>,
    NativeAgentRuntimeBackend,
    Arc<FakeProvider>,
    AgentTurnRequest,
    tempfile::TempDir,
) {
    let db = sqlite_db().await;
    let service = EmbeddedAgentService::new(Arc::clone(&db), b"inquiry-runtime-test");
    let root = tempfile::tempdir().unwrap();
    service.set_workspace_root(root.path().to_path_buf(), root.path().to_path_buf());
    let credential_id = new_uuid_v4();
    service
        .protected_store()
        .create_credential(
            &credential_id,
            "user-1",
            "openai",
            "scripted inquiry",
            Secret::new("unused"),
            &now_rfc3339(),
        )
        .await
        .unwrap();
    let (identity_id, _) = native_identity(&db, &credential_id).await;
    let session = service
        .create_inquiry_session("user-1", &identity_id)
        .await
        .unwrap();
    let resumed = service
        .create_inquiry_session("user-1", &identity_id)
        .await
        .unwrap();
    assert_eq!(session.id, resumed.id, "authority metadata may be reused");
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        scripts,
    ));
    let backend = NativeAgentRuntimeBackend::new(service.protected_store())
        .with_provider_override(provider.clone());
    let request = AgentTurnRequest {
        forge_session_id: session.id,
        runtime_session_id: session.runtime_session_id.unwrap(),
        scope: CanonicalScope {
            scope_type: CanonicalScopeType::Account,
            scope_id: "user-1".to_owned(),
            workspace_access: WorkspaceAccess::AccountScratch,
        },
        workspace_path: Some(
            service
                .main_agent_workspace("user-1")
                .await
                .unwrap()
                .to_string_lossy()
                .into_owned(),
        ),
        provider: NativeProviderConfig {
            provider: "openai".to_owned(),
            base_url: "https://unused.invalid/v1".to_owned(),
            model: "fake".to_owned(),
            credential_handle_id: credential_id,
            owner_user_id: "user-1".to_owned(),
            provider_account_id: None,
            context_tokens: 32_768,
            max_input_tokens: 24_576,
            max_output_tokens: 1_024,
        },
        system_prompt: Some("Answer this inquiry only.".to_owned()),
        history: Vec::new(),
        input: "first-inquiry-question".to_owned(),
        cancellation: CancellationToken::new(),
    };
    (db, backend, provider, request, root)
}

#[tokio::test]
async fn native_inquiry_reused_authority_keeps_history_and_usage_fresh() {
    let scripts = ["first-inquiry-answer", "second-inquiry-answer"]
        .into_iter()
        .map(|text| {
            ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: text.to_owned(),
                },
                usage_event(10, 3),
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ])
        })
        .collect();
    let (db, backend, provider, mut request, _root) = inquiry_fixture(scripts).await;
    let capabilities = backend.capabilities(&request.scope);
    assert!(!capabilities.persistent_session);
    assert!(!capabilities.protected_checkpoints);
    assert!(!capabilities.lcm);
    for (index, expected_answer) in ["first-inquiry-answer", "second-inquiry-answer"]
        .into_iter()
        .enumerate()
    {
        request.input = if index == 0 {
            "first-inquiry-question"
        } else {
            "second-inquiry-question"
        }
        .to_owned();
        let output = backend
            .run_turn(request.clone(), Arc::new(NoopSink))
            .await
            .unwrap();
        assert_eq!(output.text, expected_answer);
        assert_eq!(output.input_tokens, 10);
        assert_eq!(output.output_tokens, 3);
    }
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    let second = serde_json::to_string(&requests[1].messages).unwrap();
    assert!(second.contains("second-inquiry-question"));
    assert!(!second.contains("first-inquiry-question"));
    assert!(!second.contains("first-inquiry-answer"));
    let stored: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM protected_agent_session_state WHERE session_id = ?",
    )
    .bind(&request.forge_session_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        stored, 0,
        "ephemeral inquiries never persist restorable conversation state"
    );
}

#[tokio::test]
async fn native_inquiry_refreshes_stale_capabilities_without_deleting_the_old_session() {
    let (db, _backend, _provider, request, root) = inquiry_fixture(vec![]).await;
    let old = db::AgentSessionRepo::get_agent_session(&*db, &request.forge_session_id)
        .await
        .unwrap()
        .unwrap();
    let mut stale: forge_agent_host::BackendCapabilities =
        serde_json::from_str(&old.capabilities_json).unwrap();
    stale.persistent_session = true;
    stale.protected_checkpoints = true;
    stale.lcm = true;
    sqlx::query(
        "UPDATE agent_session SET capabilities_json = ?, version = version + 1 WHERE id = ?",
    )
    .bind(serde_json::to_string(&stale).unwrap())
    .bind(&old.id)
    .execute(db.pool())
    .await
    .unwrap();
    let service = EmbeddedAgentService::new(Arc::clone(&db), b"inquiry-runtime-test");
    service.set_workspace_root(root.path().to_path_buf(), root.path().to_path_buf());
    let refreshed = service
        .create_inquiry_session("user-1", &old.identity_id)
        .await
        .unwrap();
    assert_ne!(refreshed.id, old.id);
    assert_eq!(
        refreshed.predecessor_session_id.as_deref(),
        Some(old.id.as_str())
    );
    let capabilities: forge_agent_host::BackendCapabilities =
        serde_json::from_str(&refreshed.capabilities_json).unwrap();
    assert!(
        !capabilities.persistent_session
            && !capabilities.protected_checkpoints
            && !capabilities.lcm
    );
    assert!(db::AgentSessionRepo::get_agent_session(&*db, &old.id)
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn native_inquiry_cancellation_drains_and_unregisters_the_turn() {
    let (_db, backend, provider, request, _root) =
        inquiry_fixture(vec![ScriptedStream::blocking(vec![])]).await;
    let cancellation = request.cancellation.clone();
    let runtime_id = request.runtime_session_id.clone();
    let running_backend = backend.clone();
    let running =
        tokio::spawn(async move { running_backend.run_turn(request, Arc::new(NoopSink)).await });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while provider.requests().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cancellation.cancel();
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), running)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_err());
    assert!(matches!(
        backend.cancel(&runtime_id).await,
        Err(forge_agent_host::AgentHostError::SessionNotFound)
    ));
}

#[tokio::test]
async fn native_inquiry_dropped_backend_cancels_and_unregisters_the_turn() {
    let (_db, backend, provider, request, _root) =
        inquiry_fixture(vec![ScriptedStream::blocking(vec![])]).await;
    let runtime_id = request.runtime_session_id.clone();
    let duplicate = request.clone();
    let running_backend = backend.clone();
    let running =
        tokio::spawn(async move { running_backend.run_turn(request, Arc::new(NoopSink)).await });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while provider.requests().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let conflict = backend
        .run_turn(duplicate, Arc::new(NoopSink))
        .await
        .unwrap_err();
    assert!(conflict.to_string().contains("already active"));
    assert_eq!(
        provider.requests().len(),
        1,
        "an overlapping turn must never reach the provider"
    );
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !matches!(
            backend.cancel(&runtime_id).await,
            Err(forge_agent_host::AgentHostError::SessionNotFound)
        ) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropped turn cleanup must unregister after shutdown");
}

fn scripted_reply_provider(turns: usize) -> FakeProvider {
    let reply = "The assistant continues the plan with one more considered step. ".repeat(50);
    let events = vec![
        ProviderStreamEvent::TextDelta { text: reply },
        usage_event(2_000, 800),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ];
    FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        (0..turns)
            .map(|_| ScriptedStream::new(events.clone()))
            .collect(),
    )
}

async fn lcm_counts(db: &SqliteDb, timeline_id: &str) -> (i64, i64, i64) {
    let entries: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_lcm_entry WHERE timeline_id = ?")
            .bind(timeline_id)
            .fetch_one(db.pool())
            .await
            .expect("entry count");
    let leaf_nodes: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_lcm_node WHERE timeline_id = ? AND kind = 'leaf'",
    )
    .bind(timeline_id)
    .fetch_one(db.pool())
    .await
    .expect("leaf node count");
    let condensed_nodes: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_lcm_node WHERE timeline_id = ? AND kind = 'condensed'",
    )
    .bind(timeline_id)
    .fetch_one(db.pool())
    .await
    .expect("condensed node count");
    (entries, leaf_nodes, condensed_nodes)
}

/// The planner must fit the system prompt, tool schemas, and the new user
/// input alongside conversation history, while LCM pressure counts only the
/// timeline. This case sits in the former dead zone: history alone is far
/// below the hard threshold of the full window, but the planned total
/// overflows it. Before the host deducted non-conversation overhead from the
/// coordinator's budget, this failed planner-side (`budget_exceeded`) without
/// LCM ever compacting; now pressure trips early and the turn completes.
#[tokio::test]
async fn native_chat_compacts_when_system_prompt_crowds_the_window() {
    let db = sqlite_db().await;
    let service = EmbeddedAgentService::new(Arc::clone(&db), b"lcm-compaction-test-key");
    let credential_id = new_uuid_v4();
    service
        .protected_store()
        .create_credential(
            &credential_id,
            "user-1",
            "openai",
            "scripted provider",
            Secret::new("unused-test-key"),
            &now_rfc3339(),
        )
        .await
        .expect("credential creates");
    let (identity_id, profile_id) = native_identity(&db, &credential_id).await;
    let chats = AgentChatService::new(Arc::clone(&db));
    chats
        .set_main_binding(SetMainAgentBindingInput {
            actor_user_id: "user-1".to_owned(),
            account_id: "user-1".to_owned(),
            identity_id: identity_id.clone(),
            autonomy_policy_json: "{}".to_owned(),
            tool_policy_revision: "test".to_owned(),
            expected_version: None,
            replacement_reason: None,
        })
        .await
        .expect("Main binding");
    let chat = db::AgentChatRepo::get_main_chat(&*db, "user-1")
        .await
        .expect("Main chat lookup")
        .expect("Main chat");
    let session = service
        .create_or_resume_session(CreateScopedSession {
            actor_user_id: "user-1".to_owned(),
            identity_id: identity_id.clone(),
            profile_id: Some(profile_id.clone()),
            scope: RequestedCanonicalScope::AgentChat {
                chat_id: chat.id.clone(),
            },
        })
        .await
        .expect("session creates");
    let runtime_session_id = session
        .runtime_session_id
        .clone()
        .expect("native session has a runtime id");
    let backend = NativeAgentRuntimeBackend::new(service.protected_store())
        .with_provider_override(Arc::new(scripted_reply_provider(4)));

    // ~28k chars ≈ 7k tokens of operating-skill-style instructions: the shape
    // of a Project Agent scope where fixed content claims most of the window.
    let system_prompt = format!(
        "You are the Project Agent. {}",
        "Follow the operating skill exactly as written here. ".repeat(560)
    );
    // ~25k chars ≈ 6.2k tokens of history: far below 95% of the 12,288-token
    // window on its own, but the planned total (with the system prompt) is
    // well over it.
    let mut seed_history = Vec::new();
    for index in 0..14 {
        seed_history.push(Message::user(format!(
            "user message {index}: {}",
            "considered planning detail. ".repeat(32)
        )));
        seed_history.push(Message::text(
            Role::Assistant,
            format!(
                "assistant reply {index}: {}",
                "prior assistant reasoning. ".repeat(32)
            ),
        ));
    }

    let provider_config = NativeProviderConfig {
        provider: "openai".to_owned(),
        base_url: "https://unused.invalid/v1".to_owned(),
        model: "fake".to_owned(),
        credential_handle_id: credential_id.clone(),
        owner_user_id: "user-1".to_owned(),
        provider_account_id: None,
        context_tokens: 24_576,
        max_input_tokens: 12_288,
        max_output_tokens: 1_024,
    };
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::AgentChat,
        scope_id: chat.id.clone(),
        workspace_access: WorkspaceAccess::Deny,
    };

    let mut timeline_id = None;
    for turn in 0..3 {
        let output = backend
            .run_turn(
                AgentTurnRequest {
                    forge_session_id: session.id.clone(),
                    runtime_session_id: runtime_session_id.clone(),
                    scope: scope.clone(),
                    workspace_path: None,
                    provider: provider_config.clone(),
                    system_prompt: Some(system_prompt.clone()),
                    history: if turn == 0 {
                        seed_history.clone()
                    } else {
                        Vec::new()
                    },
                    input: format!("turn {turn}: continue the plan"),
                    cancellation: CancellationToken::new(),
                },
                Arc::new(NoopSink),
            )
            .await
            .unwrap_or_else(|error| {
                panic!("turn {turn} must compact instead of failing planner-side: {error}")
            });
        assert!(
            !output.text.trim().is_empty(),
            "turn {turn} returns assistant text"
        );
        let manifest = output
            .context_manifest
            .expect("native turn links a runtime context manifest");
        timeline_id = Some(
            manifest
                .lcm_timeline_id
                .clone()
                .expect("manifest links the chat LCM timeline"),
        );
    }

    let timeline_id = timeline_id.expect("at least one turn ran");
    let (entries, leaf_nodes, condensed_nodes) = lcm_counts(&db, &timeline_id).await;
    assert!(entries > 0, "canonical history is admitted as LCM entries");
    assert!(
        leaf_nodes + condensed_nodes > 0,
        "the crowded window must condense history into LCM nodes \
         (leaf: {leaf_nodes}, condensed: {condensed_nodes})"
    );
}

#[tokio::test]
async fn native_main_chat_compacts_over_budget_history_through_lcm() {
    let db = sqlite_db().await;
    let service = EmbeddedAgentService::new(Arc::clone(&db), b"lcm-compaction-test-key");
    let credential_id = new_uuid_v4();
    service
        .protected_store()
        .create_credential(
            &credential_id,
            "user-1",
            "openai",
            "scripted provider",
            Secret::new("unused-test-key"),
            &now_rfc3339(),
        )
        .await
        .expect("credential creates");
    let (identity_id, profile_id) = native_identity(&db, &credential_id).await;
    let chats = AgentChatService::new(Arc::clone(&db));
    chats
        .set_main_binding(SetMainAgentBindingInput {
            actor_user_id: "user-1".to_owned(),
            account_id: "user-1".to_owned(),
            identity_id: identity_id.clone(),
            autonomy_policy_json: "{}".to_owned(),
            tool_policy_revision: "test".to_owned(),
            expected_version: None,
            replacement_reason: None,
        })
        .await
        .expect("Main binding");
    let chat = db::AgentChatRepo::get_main_chat(&*db, "user-1")
        .await
        .expect("Main chat lookup")
        .expect("Main chat");
    let session = service
        .create_or_resume_session(CreateScopedSession {
            actor_user_id: "user-1".to_owned(),
            identity_id: identity_id.clone(),
            profile_id: Some(profile_id.clone()),
            scope: RequestedCanonicalScope::AgentChat {
                chat_id: chat.id.clone(),
            },
        })
        .await
        .expect("session creates");
    let runtime_session_id = session
        .runtime_session_id
        .clone()
        .expect("native session has a runtime id");

    // The same protected store the service composed, with only the outbound
    // provider replaced; everything else is the production turn path.
    let backend = NativeAgentRuntimeBackend::new(service.protected_store())
        .with_provider_override(Arc::new(scripted_reply_provider(6)));

    // A transcript slightly over the hard pressure threshold: ~52k chars is
    // roughly 13k tokens under the 4-chars-per-token sizer against a
    // 12,288-token input budget (hard threshold 95% ≈ 11.7k), the shape of a
    // chat that has just outgrown its window. Bounded hard compaction absorbs
    // this overshoot; a transcript several times the budget in one jump is
    // instead refused after `max_rounds`, which the runtime reports as
    // "LCM context cannot fit after bounded hard compaction".
    let mut seed_history = Vec::new();
    for index in 0..28 {
        seed_history.push(Message::user(format!(
            "user message {index}: {}",
            "considered planning detail. ".repeat(32)
        )));
        seed_history.push(Message::text(
            Role::Assistant,
            format!(
                "assistant reply {index}: {}",
                "prior assistant reasoning. ".repeat(32)
            ),
        ));
    }
    // One canonical turn far larger than the stock 2048-token leaf target
    // (~12k chars ≈ 3k tokens): leaf planning must swallow the whole
    // `[user, assistant]` pair in one span instead of backing up to the
    // previous user boundary forever, which wedged the condensation frontier
    // in front of any long assistant reply.
    seed_history.push(Message::user(
        "please write the full portfolio review".to_owned(),
    ));
    seed_history.push(Message::text(
        Role::Assistant,
        format!(
            "the full review: {}",
            "an exhaustive portfolio finding. ".repeat(380)
        ),
    ));

    let provider_config = NativeProviderConfig {
        provider: "openai".to_owned(),
        base_url: "https://unused.invalid/v1".to_owned(),
        model: "fake".to_owned(),
        credential_handle_id: credential_id.clone(),
        owner_user_id: "user-1".to_owned(),
        provider_account_id: None,
        context_tokens: 24_576,
        max_input_tokens: 12_288,
        max_output_tokens: 1_024,
    };
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::AgentChat,
        scope_id: chat.id.clone(),
        workspace_access: WorkspaceAccess::Deny,
    };

    let mut compaction_turn = None;
    let mut linked_timeline_id = None;
    for turn in 0..4 {
        let attempt = backend
            .run_turn(
                AgentTurnRequest {
                    forge_session_id: session.id.clone(),
                    runtime_session_id: runtime_session_id.clone(),
                    scope: scope.clone(),
                    workspace_path: None,
                    provider: provider_config.clone(),
                    system_prompt: Some(
                        "You are the account Main Agent in an LCM compaction test.".to_owned(),
                    ),
                    history: if turn == 0 {
                        seed_history.clone()
                    } else {
                        Vec::new()
                    },
                    input: format!("turn {turn}: continue the plan"),
                    cancellation: CancellationToken::new(),
                },
                Arc::new(NoopSink),
            )
            .await;
        let output = match attempt {
            Ok(output) => output,
            Err(error) => {
                let timelines: Vec<(String, String, String)> =
                    sqlx::query_as("SELECT id, scope_type, scope_id FROM agent_lcm_timeline")
                        .fetch_all(db.pool())
                        .await
                        .expect("timeline dump");
                for (id, scope_type, scope_id) in &timelines {
                    let (entries, leaf_nodes, condensed_nodes) = lcm_counts(&db, id).await;
                    eprintln!(
                        "DEBUG timeline {id} ({scope_type}/{scope_id}): entries={entries} leaf={leaf_nodes} condensed={condensed_nodes}"
                    );
                }
                let nodes: Vec<(String, i64, i64, i64, i64)> = sqlx::query_as(
                    "SELECT kind, range_start, range_end, token_count, source_token_count
                     FROM agent_lcm_node ORDER BY range_start",
                )
                .fetch_all(db.pool())
                .await
                .expect("node dump");
                eprintln!("DEBUG nodes (kind, range, tokens, source_tokens): {nodes:?}");
                panic!("native Agent Chat turn {turn} failed: {error}");
            }
        };
        assert!(
            !output.text.trim().is_empty(),
            "turn {turn} returns assistant text"
        );
        let manifest = output
            .context_manifest
            .expect("native turn links a runtime context manifest");
        let timeline_id = manifest
            .lcm_timeline_id
            .clone()
            .expect("manifest links the chat LCM timeline");
        linked_timeline_id = Some(timeline_id.clone());
        let (_, leaf_nodes, condensed_nodes) = lcm_counts(&db, &timeline_id).await;
        if leaf_nodes + condensed_nodes > 0 {
            compaction_turn = Some(turn);
            assert!(
                !manifest.summaries.is_empty(),
                "the compacting turn's manifest records summary coverage"
            );
            break;
        }
    }

    let timeline_id = linked_timeline_id.expect("at least one turn ran");
    let compaction_turn = compaction_turn.expect(
        "hard pressure over a 12k-token budget must condense the seeded history into LCM nodes",
    );

    // The manifest-linked timeline is the canonical chat-scoped timeline.
    let (stored_scope_type, stored_scope_id): (String, String) =
        sqlx::query_as("SELECT scope_type, scope_id FROM agent_lcm_timeline WHERE id = ?")
            .bind(&timeline_id)
            .fetch_one(db.pool())
            .await
            .expect("timeline row");
    assert_eq!(stored_scope_type, "agent_chat");
    assert_eq!(stored_scope_id, chat.id);

    let (entries, leaf_nodes, condensed_nodes) = lcm_counts(&db, &timeline_id).await;
    assert!(entries > 0, "canonical history is admitted as LCM entries");
    assert!(
        leaf_nodes + condensed_nodes > 0,
        "compaction produced summary nodes (leaf: {leaf_nodes}, condensed: {condensed_nodes})"
    );

    // The chat survives compaction: the next turn still completes normally on
    // the persisted runtime session.
    let output = backend
        .run_turn(
            AgentTurnRequest {
                forge_session_id: session.id.clone(),
                runtime_session_id: runtime_session_id.clone(),
                scope,
                workspace_path: None,
                provider: provider_config,
                system_prompt: Some(
                    "You are the account Main Agent in an LCM compaction test.".to_owned(),
                ),
                history: Vec::new(),
                input: format!("turn {}: continue after compaction", compaction_turn + 1),
                cancellation: CancellationToken::new(),
            },
            Arc::new(NoopSink),
        )
        .await
        .expect("the turn after compaction completes");
    assert!(
        !output.text.trim().is_empty(),
        "the turn after compaction returns assistant text"
    );
}
