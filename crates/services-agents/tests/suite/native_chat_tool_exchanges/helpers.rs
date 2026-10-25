//! Shared native chat fixtures, scripted providers, and metered turn helpers.

use super::*;

/// The exact id observed answering two different tools in one live session.
pub(super) const REUSED_ID: &str = "call_130845";

#[derive(Debug)]
pub(super) struct NoopSink;

#[async_trait::async_trait]
impl TurnEventSink for NoopSink {}

pub(super) async fn sqlite_db() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let now = now_rfc3339();
    UserRepo::create_user(
        &*db,
        &User {
            id: "user-1".to_owned(),
            email: "user-1@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("user creates");
    db
}

pub(super) async fn native_identity(
    db: &SqliteDb,
    credential_id: &str,
    account_permission_ceiling: serde_json::Value,
    tool_policy: serde_json::Value,
) -> (String, String) {
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
            account_permission_ceiling: account_permission_ceiling.to_string(),
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
            tool_policy_json: tool_policy.to_string(),
            config_json: serde_json::json!({"base_url": "https://unused.invalid/v1"}).to_string(),
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
pub(super) fn tool_call_step(id: &str, name: &str) -> ScriptedStream {
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
pub(super) fn text_step(text: &str) -> ScriptedStream {
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

pub(super) fn scripted_provider(steps: Vec<ScriptedStream>) -> Arc<FakeProvider> {
    Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        steps,
    ))
}

/// Call and result counts per tool-call id over the durable timeline, read the
/// way the context planner reads it: exactly one of each is required.
pub(super) async fn durable_pairings(
    db: &SqliteDb,
    timeline_id: &str,
) -> BTreeMap<String, (usize, usize)> {
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

pub(super) struct ChatFixture {
    pub(super) db: Arc<SqliteDb>,
    pub(super) service: EmbeddedAgentService,
    pub(super) session_id: String,
    pub(super) runtime_session_id: String,
    pub(super) provider_config: NativeProviderConfig,
    pub(super) scope: CanonicalScope,
}

pub(super) async fn chat_fixture() -> ChatFixture {
    chat_fixture_with_policy(
        serde_json::json!({ "permissions": ["read_agent_chat", "read_memory"] }),
        serde_json::json!({ "allowed": ["read_agent_chat", "read_memory"] }),
    )
    .await
}

pub(super) async fn chat_fixture_with_policy(
    account_permission_ceiling: serde_json::Value,
    tool_policy: serde_json::Value,
) -> ChatFixture {
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
    let (identity_id, profile_id) =
        native_identity(&db, &credential_id, account_permission_ceiling, tool_policy).await;
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
    pub(super) fn turn(&self, input: &str) -> AgentTurnRequest {
        self.turn_with_history(input, Vec::new())
    }

    pub(super) fn turn_with_history(&self, input: &str, history: Vec<Message>) -> AgentTurnRequest {
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
            server_state_card: None,
            command_allowlist: None,
            environment: Default::default(),
            cancellation: CancellationToken::new(),
        }
    }
}

/// Points the bound Main Agent's profile at an endpoint the scripted provider
/// replaces, which the worker path requires before it admits a native turn.
#[cfg(feature = "test-support")]
pub(super) async fn main_agent_with_native_endpoint(fixture: &ChatFixture) -> db::Agent {
    let identity = db::AccountMainAgentBindingRepo::get_active_main_binding(&*fixture.db, "user-1")
        .await
        .unwrap()
        .unwrap();
    let agent = AgentRepo::get_by_id(&*fixture.db, &identity.identity_id)
        .await
        .unwrap()
        .unwrap();
    AgentRepo::update(
        &*fixture.db,
        db::UpdateAgent {
            id: agent.id.clone(),
            expected_version: agent.version,
            config_json: Some(r#"{"base_url":"https://unused.invalid/v1"}"#.into()),
            name: None,
            description: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: None,
            daemon_id: None,
            max_concurrent_tasks: None,
            heartbeat_interval_seconds: None,
            max_missed_heartbeats: None,
            status: None,
            last_heartbeat_at: None,
            is_default: None,
            paused: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    agent
}

/// A plain text reply for every provider call, for cases where the pressure
/// decision — not the tool loop — is what is under test.
pub(super) fn text_only_provider(turns: usize) -> Arc<FakeProvider> {
    let reply = "The assistant continues the plan with one more considered step. ".repeat(20);
    Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        (0..turns).map(|_| text_step(&reply)).collect(),
    ))
}

#[cfg(feature = "test-support")]
#[derive(Debug)]
pub(super) struct UnreachableCli;
#[cfg(feature = "test-support")]
#[async_trait::async_trait]
impl executors::TaskExecutor for UnreachableCli {
    async fn execute(
        &self,
        _: executors::ExecutionContext,
    ) -> Result<executors::ExecutionResult, executors::ExecutorError> {
        panic!("native turn must not invoke CLI");
    }
    async fn cancel(&self, _: &str) -> Result<(), executors::ExecutorError> {
        panic!("native turn must not invoke CLI");
    }
}

// ---------------------------------------------------------------------------
// Usage ledger: every provider call is recorded exactly once.
//
// A chat's runtime session is persistent, and the runtime's usage ledger
// accumulates for the life of that session. The host used to report the whole
// ledger after each turn, so turn n recorded n calls (its own plus every
// earlier turn's) and an n-turn chat was billed 1 + 2 + … + n calls.
// ---------------------------------------------------------------------------

/// The measured per-call usage of the live chat the over-count was found on.
pub(super) const MEASURED_CALLS: [(u64, u64); 4] =
    [(5_162, 496), (5_679, 683), (6_381, 257), (6_688, 181)];

pub(super) fn metered_text_step(text: &str, (input, output): (u64, u64)) -> ScriptedStream {
    ScriptedStream::new(vec![
        ProviderStreamEvent::TextDelta {
            text: text.to_owned(),
        },
        usage_event(input, output),
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ])
}

pub(super) fn metered_tool_call_step(
    id: &str,
    name: &str,
    (input, output): (u64, u64),
) -> ScriptedStream {
    ScriptedStream::new(vec![
        ProviderStreamEvent::ToolCallDelta {
            index: 0,
            id: Some(id.to_owned()),
            name: Some(name.to_owned()),
            arguments_fragment: "{}".to_owned(),
        },
        usage_event(input, output),
        ProviderStreamEvent::Finish {
            reason: FinishReason::ToolCalls,
        },
    ])
}

#[cfg(feature = "test-support")]
pub(super) struct LedgerChat {
    pub(super) db: Arc<SqliteDb>,
    pub(super) chat_id: String,
    pub(super) chats: AgentChatService<SqliteDb>,
    pub(super) worker: services::AgentChatTurnWorker,
    pub(super) provider: Arc<FakeProvider>,
    pub(super) _logs: tempfile::TempDir,
}

/// A Main Chat whose turns run through the real worker, runner, native host
/// and ledger settlement against a scripted provider priced at $1 per million
/// input tokens and $2 per million output tokens.
#[cfg(feature = "test-support")]
pub(super) async fn ledger_chat(steps: Vec<ScriptedStream>) -> LedgerChat {
    use pricing::PricingCatalogRepository;
    use std::time::{Duration, SystemTime};

    let fixture = chat_fixture().await;
    let binding = db::AccountMainAgentBindingRepo::get_active_main_binding(&*fixture.db, "user-1")
        .await
        .unwrap()
        .unwrap();
    let agent = AgentRepo::get_by_id(&*fixture.db, &binding.identity_id)
        .await
        .unwrap()
        .unwrap();
    AgentRepo::update(
        &*fixture.db,
        db::UpdateAgent {
            id: agent.id.clone(),
            expected_version: agent.version,
            config_json: Some(r#"{"base_url":"https://unused.invalid/v1"}"#.into()),
            name: None,
            description: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: None,
            daemon_id: None,
            max_concurrent_tasks: None,
            heartbeat_interval_seconds: None,
            max_missed_heartbeats: None,
            status: None,
            last_heartbeat_at: None,
            is_default: None,
            paused: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let when = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let snapshot = pricing::parse_models_dev_catalog(
        br#"{"openai": {"id":"openai","name":"OpenAI","models":{
              "fake":{"id":"fake","last_updated":"2026-09-01",
                "cost":{"input":1,"output":2}}}}}"#,
    )
    .expect("catalog parses")
    .into_snapshot("usage-ledger-snapshot", None, when, when)
    .expect("snapshot materializes");
    pricing::sqlite::SqlitePricingRepository::new(fixture.db.clone())
        .activate_catalog_snapshot(snapshot, "usage-ledger-refresh")
        .await
        .expect("catalog activates");

    let provider = scripted_provider(steps);
    let backend = NativeAgentRuntimeBackend::new(fixture.service.protected_store())
        .with_provider_override(provider.clone());
    let logs = tempfile::tempdir().unwrap();
    let runner = services::FederatedAgentChatTurnRunner::new(
        fixture.db.clone(),
        Arc::new(fixture.service.with_native_backend(Arc::new(backend))),
        Arc::new(UnreachableCli),
        services::AgentChatTurnLogRoot::new(logs.path()),
    );
    LedgerChat {
        chats: AgentChatService::new(fixture.db.clone()),
        worker: services::AgentChatTurnWorker::with_runner(fixture.db.clone(), Arc::new(runner)),
        db: fixture.db,
        chat_id: fixture.scope.scope_id,
        provider,
        _logs: logs,
    }
}

/// One recorded provider call: `(attempt_ordinal, report_sequence, input,
/// output, estimated nano-USD)`.
#[cfg(feature = "test-support")]
pub(super) type RecordedCall = (i64, i64, i64, i64, Option<i64>);

/// What `(input, output)` costs at the fixture's rates, in nano-USD.
#[cfg(feature = "test-support")]
pub(super) fn nano_usd((input, output): (u64, u64)) -> i64 {
    i64::try_from(input * 1_000 + output * 2_000).unwrap()
}

#[cfg(feature = "test-support")]
pub(super) fn recorded(
    attempt_ordinal: i64,
    report_sequence: i64,
    call: (u64, u64),
) -> RecordedCall {
    (
        attempt_ordinal,
        report_sequence,
        i64::try_from(call.0).unwrap(),
        i64::try_from(call.1).unwrap(),
        Some(nano_usd(call)),
    )
}

#[cfg(feature = "test-support")]
impl LedgerChat {
    /// Admit one user message and run its turn to a terminal state.
    pub(super) async fn turn(&self, content: &str) -> db::AgentChatTurnJob {
        let admitted = self
            .chats
            .send_message(services::SendAgentChatMessageInput {
                actor_user_id: "user-1".into(),
                chat_id: self.chat_id.clone(),
                content: content.into(),
                dedupe_key: Some(content.into()),
            })
            .await
            .expect("message admits");
        assert_eq!(self.worker.run_once().await.unwrap(), 1, "{content}");
        self.job(&admitted.turn_job.id).await
    }

    pub(super) async fn job(&self, id: &str) -> db::AgentChatTurnJob {
        db::AgentChatTurnJobRepo::get_agent_chat_turn_job(&*self.db, id)
            .await
            .unwrap()
            .unwrap()
    }

    /// The usage events recorded for one turn, across all of its attempts.
    pub(super) async fn recorded_calls(&self, turn_job_id: &str) -> Vec<RecordedCall> {
        sqlx::query_as(
            "SELECT e.attempt_ordinal, e.report_sequence, e.input_tokens, e.output_tokens,
                    e.estimated_nano_usd
             FROM usage_event e
             WHERE e.source_id = ?
             ORDER BY e.attempt_ordinal, e.report_sequence",
        )
        .bind(turn_job_id)
        .fetch_all(self.db.pool())
        .await
        .expect("usage events")
    }

    /// Asserts every total a user can read equals `calls` counted once each:
    /// the raw ledger, the account analytics behind
    /// `GET /api/v1/analytics/usage`, and its Main Chat surface row.
    /// `turn_attempts` is the number of turn attempts that reached the
    /// provider, which is what `provider_attempt_count` reports.
    pub(super) async fn assert_totals_count_each_call_once(
        &self,
        calls: &[(u64, u64)],
        chat_turns: i64,
        turn_attempts: i64,
    ) {
        let input = i64::try_from(calls.iter().map(|call| call.0).sum::<u64>()).unwrap();
        let output = i64::try_from(calls.iter().map(|call| call.1).sum::<u64>()).unwrap();
        let cost: i64 = calls.iter().copied().map(nano_usd).sum();
        let events = i64::try_from(calls.len()).unwrap();

        let ledger: (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT COUNT(*), SUM(input_tokens), SUM(output_tokens), SUM(estimated_nano_usd)
             FROM usage_event",
        )
        .fetch_one(self.db.pool())
        .await
        .unwrap();
        assert_eq!(ledger, (events, input, output, cost), "raw ledger totals");

        let account =
            db::UsageAnalyticsRepo::get_account_usage_analytics(&*self.db, "user-1", None, None)
                .await
                .expect("account analytics");
        let expected_tokens = api_types::TokenCounters {
            input_tokens: input,
            output_tokens: output,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
        };
        let expected_cost = Some(api_types::MoneyAmount {
            currency: "USD".to_owned(),
            decimal: nano_decimal(cost),
        });
        assert_eq!(account.token_usage.tokens, expected_tokens);
        assert_eq!(account.token_usage.counts.chat_turn_count, chat_turns);
        assert_eq!(
            account.token_usage.counts.provider_attempt_count,
            turn_attempts
        );
        assert_eq!(account.token_usage.cost.estimated, expected_cost);
        assert_eq!(account.token_usage.cost.complete_total, expected_cost);
        assert_eq!(account.token_usage.by_surface.len(), 1);
        let main_chat = &account.token_usage.by_surface[0];
        assert_eq!(main_chat.surface, api_types::UsageSurface::MainChat);
        assert_eq!(main_chat.tokens, expected_tokens);
        assert_eq!(main_chat.cost.complete_total, expected_cost);
    }
}

/// Canonical decimal USD text for a nano-USD amount.
#[cfg(feature = "test-support")]
pub(super) fn nano_decimal(nanos: i64) -> String {
    let text = format!("{}.{:09}", nanos / 1_000_000_000, nanos % 1_000_000_000);
    text.trim_end_matches('0').trim_end_matches('.').to_owned()
}

/// The Project Agent Chat of a fresh Project, answered by the fixture's
/// native identity with Project read permissions.
pub(super) async fn project_chat_fixture() -> ChatFixture {
    let mut fixture = chat_fixture_with_policy(
        serde_json::json!({"permissions": ["read_project", "read_agent_chat", "read_memory"]}),
        serde_json::json!({"allowed": ["read_project", "read_agent_chat", "read_memory"]}),
    )
    .await;
    let project = new_uuid_v4();
    let now = now_rfc3339();
    ProjectRepo::create(
        &*fixture.db,
        CreateProject {
            id: project.clone(),
            name: "Measurement Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some("user-1".to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let project_chat: String = sqlx::query_scalar("SELECT id FROM agent_chat WHERE project_id = ?")
        .bind(&project)
        .fetch_one(fixture.db.pool())
        .await
        .unwrap();
    let original = db::AgentSessionRepo::get_agent_session(&*fixture.db, &fixture.session_id)
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE project_agent_binding SET identity_id = ?, profile_id = ?, state = 'active', permission_ceiling_json = ? WHERE project_id = ?")
        .bind(&original.identity_id).bind(&original.profile_id)
        .bind(serde_json::json!({"allowed": ["read_project", "read_agent_chat", "read_memory"]}).to_string())
        .bind(&project).execute(fixture.db.pool()).await.unwrap();
    sqlx::query("INSERT OR IGNORE INTO project_member (id, project_id, user_id, role, created_at, updated_at) VALUES (?, ?, 'user-1', 'owner', ?, ?)")
        .bind(new_uuid_v4()).bind(&project).bind(&now).bind(&now).execute(fixture.db.pool()).await.unwrap();
    let session = fixture
        .service
        .create_or_resume_session(CreateScopedSession {
            actor_user_id: "user-1".into(),
            identity_id: original.identity_id,
            profile_id: Some(original.profile_id),
            scope: RequestedCanonicalScope::AgentChat {
                chat_id: project_chat.clone(),
            },
        })
        .await
        .unwrap();
    fixture.session_id = session.id;
    fixture.runtime_session_id = session.runtime_session_id.unwrap();
    fixture.scope.scope_id = project_chat;
    fixture
}

pub(super) fn user_texts(request: &agent_runtime::core::provider::ProviderRequest) -> Vec<String> {
    request
        .messages
        .iter()
        .filter(|message| message.role == agent_runtime::core::content::Role::User)
        .map(|message| {
            message
                .content
                .first()
                .and_then(ContentPart::as_text)
                .unwrap_or_default()
                .to_owned()
        })
        .collect()
}

pub(super) fn topic_request(chat_id: &str, label: &str) -> db::RotateAgentChatTopic {
    db::RotateAgentChatTopic {
        runtime_session_id: None,
        rotation_owner: None,
        topic: db::CreateAgentChatTopic {
            id: new_uuid_v4(),
            chat_id: chat_id.to_owned(),
            label: label.into(),
            summary: None,
            principal_type: "user".into(),
            principal_id: Some("user-1".into()),
            created_at: now_rfc3339(),
        },
        divider_message: db::topic_divider_message(
            new_uuid_v4(),
            chat_id.to_owned(),
            label,
            new_uuid_v4(),
            now_rfc3339(),
        ),
    }
}
