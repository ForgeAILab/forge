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
