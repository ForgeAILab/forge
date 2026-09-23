//! Tool exchanges on a persisted, LCM-backed Agent Chat timeline.
//!
//! Two properties, both of which a worker or Project Agent timeline — mostly
//! `[assistant tool call, tool result]` pairs — depends on: a reused
//! provider tool-call id must not poison the durable history, and pressure
//! must see what those exchanges actually cost.
//!
//! Providers do not guarantee tool-call ids are unique across a conversation
//! — z.ai answered one tool with `call_130845` and, much later in the same
//! session, a different tool with the same id. Forge's canonical history is
//! durable (`agent_lcm_entry` is append-only and immutable), so both
//! exchanges stay on the timeline forever, and the context planner requires
//! exactly one call and one result fragment per id. From the second exchange
//! on, every later turn on that timeline failed `invalid_pairing` and no
//! retry could recover: the poisoned history was replayed each attempt.
//!
//! The runtime now re-ids a colliding call at ingest, before dispatch or
//! history. These cases hold the Forge half of that contract: the collision
//! is detected against the history Forge restored — including across a fresh
//! backend process, where the only source of the earlier id is the protected
//! session store — and the durable timeline keeps exactly one call and one
//! result per id.

use std::collections::BTreeMap;
use std::sync::Arc;

use agent_runtime::core::content::ContentPart;
use agent_runtime::core::provider::{Capabilities, FinishReason, ProviderStreamEvent};
use agent_runtime::provider::fake::{usage_event, FakeProvider, ScriptedStream};
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentRepo, AgentStatus,
    CreateAgentIdentity, CreateAgentProfile, SqliteDb,
};
use forge_agent_host::{
    AgentSessionBackend, AgentTurnRequest, CanonicalScope, CanonicalScopeType, Message,
    NativeAgentRuntimeBackend, NativeProviderConfig, Secret, TurnEventSink, WorkspaceAccess,
};
use services::embedded_agent_service::{CreateScopedSession, RequestedCanonicalScope};
use services::{AgentChatService, EmbeddedAgentService, SetMainAgentBindingInput};
use tokio_util::sync::CancellationToken;

/// The exact id observed answering two different tools in one live session.
const REUSED_ID: &str = "call_130845";

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

/// One provider step that requests a single tool under `id`. The tool name is
/// deliberately one this chat scope does not compose: the runtime answers an
/// unknown tool with a canonical error result, which still puts a real
/// call/result pair on the timeline under the provider's id — the only thing
/// the pairing contract cares about.
fn tool_call_step(id: &str, name: &str) -> ScriptedStream {
    ScriptedStream::new(vec![
        ProviderStreamEvent::ToolCallDelta {
            index: 0,
            id: Some(id.to_owned()),
            name: Some(name.to_owned()),
            arguments_fragment: "{}".to_owned(),
        },
        usage_event(600, 60),
        ProviderStreamEvent::Finish {
            reason: FinishReason::ToolCalls,
        },
    ])
}

/// One provider step that closes the turn with visible text.
fn text_step(text: &str) -> ScriptedStream {
    ScriptedStream::new(vec![
        ProviderStreamEvent::TextDelta {
            text: text.to_owned(),
        },
        usage_event(600, 60),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ])
}

fn scripted_provider(steps: Vec<ScriptedStream>) -> Arc<FakeProvider> {
    Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        steps,
    ))
}

/// Call and result counts per tool-call id over the durable timeline, read the
/// way the context planner reads it: exactly one of each is required.
async fn durable_pairings(db: &SqliteDb, timeline_id: &str) -> BTreeMap<String, (usize, usize)> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT content_json FROM agent_lcm_entry WHERE timeline_id = ? ORDER BY sequence",
    )
    .bind(timeline_id)
    .fetch_all(db.pool())
    .await
    .expect("timeline entries");
    let mut pairings: BTreeMap<String, (usize, usize)> = BTreeMap::new();
    for (content_json,) in rows {
        let message: Message =
            serde_json::from_str(&content_json).expect("an LCM entry holds a canonical message");
        for part in &message.content {
            match part {
                ContentPart::ToolCall(call) => {
                    pairings.entry(call.id.as_str().to_owned()).or_default().0 += 1;
                }
                ContentPart::ToolResult(result) => {
                    pairings
                        .entry(result.call_id.as_str().to_owned())
                        .or_default()
                        .1 += 1;
                }
                _ => {}
            }
        }
    }
    pairings
}

struct ChatFixture {
    db: Arc<SqliteDb>,
    service: EmbeddedAgentService,
    session_id: String,
    runtime_session_id: String,
    provider_config: NativeProviderConfig,
    scope: CanonicalScope,
}

async fn chat_fixture() -> ChatFixture {
    let db = sqlite_db().await;
    let service = EmbeddedAgentService::new(Arc::clone(&db), b"tool-call-id-reuse-test-key");
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
            identity_id,
            profile_id: Some(profile_id),
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
    ChatFixture {
        db,
        service,
        session_id: session.id,
        runtime_session_id,
        provider_config: NativeProviderConfig {
            provider: "openai".to_owned(),
            base_url: "https://unused.invalid/v1".to_owned(),
            model: "fake".to_owned(),
            reasoning_effort: None,
            credential_handle_id: credential_id,
            owner_user_id: "user-1".to_owned(),
            provider_account_id: None,
            context_tokens: 24_576,
            max_input_tokens: 12_288,
            max_output_tokens: 1_024,
        },
        scope: CanonicalScope {
            scope_type: CanonicalScopeType::AgentChat,
            scope_id: chat.id,
            workspace_access: WorkspaceAccess::Deny,
        },
    }
}

impl ChatFixture {
    fn turn(&self, input: &str) -> AgentTurnRequest {
        self.turn_with_history(input, Vec::new())
    }

    fn turn_with_history(&self, input: &str, history: Vec<Message>) -> AgentTurnRequest {
        AgentTurnRequest {
            forge_session_id: self.session_id.clone(),
            runtime_session_id: self.runtime_session_id.clone(),
            scope: self.scope.clone(),
            workspace_path: None,
            provider: self.provider_config.clone(),
            system_prompt: Some(
                "You are the account Main Agent in a tool-call-id reuse test.".to_owned(),
            ),
            history,
            input: input.to_owned(),
            command_allowlist: None,
            cancellation: CancellationToken::new(),
        }
    }
}

/// Asserts the shape the context planner requires, and that the second
/// exchange really did land under its own id rather than being dropped.
fn assert_one_call_and_result_per_id(pairings: &BTreeMap<String, (usize, usize)>) {
    assert_eq!(
        pairings.len(),
        2,
        "two tool exchanges must occupy two distinct ids on the durable timeline: {pairings:?}"
    );
    assert!(
        pairings.contains_key(REUSED_ID),
        "the first exchange keeps the id the provider sent: {pairings:?}"
    );
    for (id, (calls, results)) in pairings {
        assert_eq!(
            (*calls, *results),
            (1, 1),
            "tool call `{id}` must have exactly one call and one result fragment: {pairings:?}"
        );
    }
}

/// The live shape: the same id answers two different tools in one chat
/// session. Every turn after the second exchange used to fail
/// `invalid_pairing`; here the chat keeps answering and the timeline stays
/// well-formed.
#[tokio::test]
async fn a_reused_tool_call_id_does_not_poison_the_chat_timeline() {
    let fixture = chat_fixture().await;
    let backend = NativeAgentRuntimeBackend::new(fixture.service.protected_store())
        .with_provider_override(scripted_provider(vec![
            tool_call_step(REUSED_ID, "forge_scope_propose"),
            text_step("scope proposed; continuing the plan."),
            tool_call_step(REUSED_ID, "forge_task_command"),
            text_step("task command attempted; continuing the plan."),
            text_step("the chat is still answering after the collision."),
        ]));

    let mut timeline_id = None;
    for (turn, input) in [
        "turn 0: propose the scope",
        "turn 1: run the task command",
        "turn 2: summarize where we are",
    ]
    .into_iter()
    .enumerate()
    {
        let output = backend
            .run_turn(fixture.turn(input), Arc::new(NoopSink))
            .await
            .unwrap_or_else(|error| panic!("turn {turn} must complete: {error}"));
        assert!(
            !output.text.trim().is_empty(),
            "turn {turn} returns assistant text"
        );
        timeline_id = output
            .context_manifest
            .expect("native turn links a runtime context manifest")
            .lcm_timeline_id;
    }

    let timeline_id = timeline_id.expect("the chat turns link an LCM timeline");
    let pairings = durable_pairings(&fixture.db, &timeline_id).await;
    assert_one_call_and_result_per_id(&pairings);
}

/// The worker shape: the colliding exchange arrives in a later process, so
/// the earlier id is only knowable from the protected session store Forge
/// restores the runtime session from. A restore that lost it would let the
/// collision through and wedge the timeline exactly as before.
#[tokio::test]
async fn a_reused_tool_call_id_is_caught_after_the_session_is_restored() {
    let fixture = chat_fixture().await;

    let first_backend = NativeAgentRuntimeBackend::new(fixture.service.protected_store())
        .with_provider_override(scripted_provider(vec![
            tool_call_step(REUSED_ID, "forge_scope_propose"),
            text_step("scope proposed in the first process."),
        ]));
    first_backend
        .run_turn(
            fixture.turn("turn 0: propose the scope"),
            Arc::new(NoopSink),
        )
        .await
        .expect("the first process' turn completes");
    drop(first_backend);

    // A fresh backend: nothing in memory remembers the first exchange.
    let second_backend = NativeAgentRuntimeBackend::new(fixture.service.protected_store())
        .with_provider_override(scripted_provider(vec![
            tool_call_step(REUSED_ID, "forge_task_command"),
            text_step("task command attempted in the second process."),
            text_step("the restored chat is still answering."),
        ]));
    let colliding = second_backend
        .run_turn(
            fixture.turn("turn 1: run the task command"),
            Arc::new(NoopSink),
        )
        .await
        .expect("the restored session's colliding turn completes");
    let timeline_id = colliding
        .context_manifest
        .expect("native turn links a runtime context manifest")
        .lcm_timeline_id
        .expect("the restored chat links an LCM timeline");
    second_backend
        .run_turn(
            fixture.turn("turn 2: summarize where we are"),
            Arc::new(NoopSink),
        )
        .await
        .expect("the turn after the collision completes");

    let pairings = durable_pairings(&fixture.db, &timeline_id).await;
    assert_one_call_and_result_per_id(&pairings);
}

/// A plain text reply for every provider call, for cases where the pressure
/// decision — not the tool loop — is what is under test.
fn text_only_provider(turns: usize) -> Arc<FakeProvider> {
    let reply = "The assistant continues the plan with one more considered step. ".repeat(20);
    Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        (0..turns).map(|_| text_step(&reply)).collect(),
    ))
}

async fn lcm_node_counts(db: &SqliteDb, timeline_id: &str) -> (i64, i64) {
    let leaf: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_lcm_node WHERE timeline_id = ? AND kind = 'leaf'",
    )
    .bind(timeline_id)
    .fetch_one(db.pool())
    .await
    .expect("leaf node count");
    let condensed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_lcm_node WHERE timeline_id = ? AND kind = 'condensed'",
    )
    .bind(timeline_id)
    .fetch_one(db.pool())
    .await
    .expect("condensed node count");
    (leaf, condensed)
}

/// Hard compaction over a history made of tool exchanges — the shape of a
/// worker or Project Agent timeline, and the one the stock sizer could not
/// see.
///
/// `CharRatioSizer::entry_tokens` charges an entry for `joined_text()` plus
/// one token per tool part, but a tool call's arguments and a tool result's
/// body sit *inside* their content part where `joined_text()` cannot reach
/// them. Measured on this exact fixture (24 exchanges, 12,288-token input
/// budget, no composed tools): it estimated 8,582 tokens against an 11,243
/// conversation budget — 76%, under the 85% hard threshold, so LCM never
/// compacted — while the planner charged 13,244 for the same history and
/// refused the turn with `budget_exceeded: input budget exceeded by 956
/// tokens ... largest category is `history` at 8750 tokens`. The serialized
/// history is ~14.5k tokens, so the estimate ran ~41% under: far past the
/// 15% of headroom `forge-lcm-pressure-3` left for estimator error. Canonical
/// history is durable, so every retry replayed it and the session never
/// recovered.
///
/// `ForgeLcmSizer` charges the serialized entry instead, which puts this
/// history over the hard threshold and compacts it.
#[tokio::test]
async fn tool_exchange_history_reaches_hard_pressure_before_the_planner_refuses_the_turn() {
    use agent_runtime::core::content::{ToolCall, ToolResultBlock};
    use agent_runtime::core::ids::ToolCallId;
    use forge_agent_host::Role;

    let fixture = chat_fixture().await;
    let backend = NativeAgentRuntimeBackend::new(fixture.service.protected_store())
        .with_provider_override(text_only_provider(6));

    // Each round is one canonical exchange: the user asks, the assistant
    // calls a tool, the runtime answers it, the assistant reports.
    let mut seed_history = Vec::new();
    for index in 0..24 {
        let call_id = ToolCallId::new(format!("call-{index}"));
        seed_history.push(Message::user(format!(
            "user message {index}: {}",
            "considered planning detail. ".repeat(24)
        )));
        seed_history.push(Message::assistant(vec![ContentPart::ToolCall(ToolCall {
            id: call_id.clone(),
            name: "forge_task_read".to_owned(),
            arguments: serde_json::json!({ "round": index }),
        })]));
        seed_history.push(Message::tool_result(ToolResultBlock {
            call_id,
            name: "forge_task_read".to_owned(),
            content: vec![ContentPart::text(format!(
                "tool result {index}: {}",
                "retrieved workspace detail. ".repeat(24)
            ))],
            is_error: false,
        }));
        seed_history.push(Message::text(
            Role::Assistant,
            format!(
                "assistant reply {index}: {}",
                "prior assistant reasoning. ".repeat(24)
            ),
        ));
    }

    let mut compacted = None;
    for turn in 0..4 {
        let history = if turn == 0 {
            seed_history.clone()
        } else {
            Vec::new()
        };
        let output = backend
            .run_turn(
                fixture.turn_with_history(&format!("turn {turn}: continue the plan"), history),
                Arc::new(NoopSink),
            )
            .await
            .unwrap_or_else(|error| {
                panic!("turn {turn} over a tool-exchange history must complete: {error}")
            });
        let timeline_id = output
            .context_manifest
            .expect("native turn links a runtime context manifest")
            .lcm_timeline_id
            .expect("manifest links the chat LCM timeline");
        let (leaf, condensed) = lcm_node_counts(&fixture.db, &timeline_id).await;
        if leaf + condensed > 0 {
            compacted = Some((turn, timeline_id));
            break;
        }
    }

    let (compaction_turn, timeline_id) = compacted.expect(
        "a tool-exchange history over the input budget must reach hard pressure and condense \
         rather than be refused by the planner",
    );

    // The turn after compaction plans against a projection that mixes summary
    // nodes with the raw suffix. It is the one that fails `invalid_pairing` if
    // a leaf span split an exchange.
    let output = backend
        .run_turn(
            fixture.turn(&format!(
                "turn {}: continue after compaction",
                compaction_turn + 1
            )),
            Arc::new(NoopSink),
        )
        .await
        .expect("the turn after compacting a tool-exchange history completes");
    assert!(
        !output.text.trim().is_empty(),
        "the turn after compaction returns assistant text"
    );

    // No half-pair may survive on the durable timeline either: LCM derives the
    // planner's pairings straight off these entries.
    let pairings = durable_pairings(&fixture.db, &timeline_id).await;
    assert_eq!(
        pairings.len(),
        24,
        "every seeded exchange is admitted under its own id: {pairings:?}"
    );
    for (id, (calls, results)) in &pairings {
        assert_eq!(
            (*calls, *results),
            (1, 1),
            "tool call `{id}` must keep exactly one call and one result entry: {pairings:?}"
        );
    }
}

/// A Forge-side LCM policy change must not wedge existing sessions.
///
/// The runtime folds Forge's sizer, pressure policy, and summary policy into
/// one LCM component revision, and `decode_state` refuses state written under
/// a different one. That conflict fails the turn and is replayed on every
/// attempt, so before the protected store tracked
/// `FORGE_LCM_POLICY_REVISION`, shipping any change to those policies — the
/// sizer fix above included — would have failed every turn of every live
/// chat with no recovery path. The component's state is a cache over the
/// durable timeline, so the host drops it and lets the coordinator rebuild.
#[tokio::test]
async fn a_superseded_lcm_policy_revision_rebuilds_instead_of_failing_every_turn() {
    use agent_runtime::core::checkpoint::{CheckpointStore, TurnCheckpoint};
    use agent_runtime::core::ids::SessionId;
    use agent_runtime::core::store::{SessionSnapshot, SessionStore};
    use agent_runtime::registry::RegistryRevision;

    let fixture = chat_fixture().await;
    let backend = NativeAgentRuntimeBackend::new(fixture.service.protected_store())
        .with_provider_override(text_only_provider(4));
    backend
        .run_turn(fixture.turn("turn 0: start the chat"), Arc::new(NoopSink))
        .await
        .expect("the first turn completes and writes LCM component state");

    let store = fixture.service.protected_store();
    let runtime_session = SessionId::new(&fixture.runtime_session_id);
    // Emulates component state written by a binary whose LCM policy differed:
    // the revision is exactly what the runtime compares and rejects. The
    // checkpoint keeps its own copy of the namespace and the resume overlay
    // reinstates it, so both copies must carry the superseded revision for
    // this to be the state a policy change really leaves behind.
    let stamp_superseded_state = || async {
        let superseded = RegistryRevision::new("superseded-lcm-component");
        let mut snapshot: SessionSnapshot = SessionStore::load(&*store, &runtime_session)
            .await
            .expect("snapshot loads")
            .expect("the first turn persisted a snapshot");
        snapshot
            .extension_state
            .get_mut("harness.lcm")
            .expect("the first turn persisted LCM component state")
            .revision = superseded.clone();
        SessionStore::save(&*store, &snapshot)
            .await
            .expect("snapshot saves");
        let mut checkpoint: TurnCheckpoint =
            CheckpointStore::load_latest(&*store, &runtime_session)
                .await
                .expect("checkpoint loads")
                .expect("the first turn persisted a checkpoint");
        checkpoint
            .snapshot
            .extension_state
            .get_mut("harness.lcm")
            .expect("the checkpoint carries LCM component state")
            .revision = superseded;
        CheckpointStore::save(&*store, &checkpoint)
            .await
            .expect("checkpoint saves");
    };

    // Control: the marker matches this binary, so the state is taken at face
    // value and the runtime rejects it — the failure mode being fixed.
    stamp_superseded_state().await;
    let error = backend
        .run_turn(
            fixture.turn("turn 1: continue over unreadable component state"),
            Arc::new(NoopSink),
        )
        .await
        .expect_err("component state this binary cannot decode must fail the turn");
    assert!(
        error.to_string().contains("LCM component revision changed"),
        "the control case must fail on the component revision, not something else: {error}"
    );

    // A policy change leaves exactly this behind: state from the old policy,
    // beside the old policy's marker.
    stamp_superseded_state().await;
    sqlx::query("UPDATE protected_agent_session_state SET lcm_policy_revision = ?")
        .bind("forge-lcm-policy-superseded")
        .execute(fixture.db.pool())
        .await
        .expect("marker updates");

    let output = backend
        .run_turn(
            fixture.turn("turn 2: continue after the policy change"),
            Arc::new(NoopSink),
        )
        .await
        .expect("a turn after a Forge LCM policy change rebuilds instead of failing");
    assert!(
        !output.text.trim().is_empty(),
        "the rebuilt session answers normally"
    );
    let timeline_id = output
        .context_manifest
        .expect("native turn links a runtime context manifest")
        .lcm_timeline_id
        .expect("the rebuilt session links the same chat LCM timeline");
    let entries: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_lcm_entry WHERE timeline_id = ?")
            .bind(&timeline_id)
            .fetch_one(fixture.db.pool())
            .await
            .expect("entry count");
    assert!(
        entries > 0,
        "the durable timeline the state was rebuilt from is intact"
    );
}
