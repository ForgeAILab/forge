use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use api_types::ProductMaturity;
use async_trait::async_trait;
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AccountMainAgentBindingRepo,
    AgentChatRepo, AgentChatTurnJob, AgentChatTurnJobRepo, AgentChatTurnState, AgentProfileRepo,
    AgentRepo, AgentStatus, CreateAgentIdentity, CreateAgentProfile, SelectAgentProfile, SqliteDb,
    User, UserRepo,
};
use forge_agent_host::{CanonicalScope, CanonicalScopeType, WorkspaceAccess};
use serde_json::json;
use services::{
    AgentChatService, AgentChatTurnRunner, AgentChatTurnWorker, CompletedAgentChatTurn,
    MainGenesisCommandService, MainGenesisStartCommandInput, MainGenesisStartPrincipal,
    MainGenesisStartRequest, SendAgentChatMessageInput, ServiceError, SetMainAgentBindingInput,
};
use tokio_util::sync::CancellationToken;

const ACCOUNT_ID: &str = "worker-retry-account";
const IDENTITY_ID: &str = "worker-retry-identity";
const PROFILE_ID: &str = "worker-retry-profile";

async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    let db = Arc::new(SqliteDb::new(pool));
    let now = now_rfc3339();
    UserRepo::create_user(
        &*db,
        &User {
            id: ACCOUNT_ID.to_owned(),
            email: "worker-retry@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Worker Retry Test".to_owned()),
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("user");
    create_identity_with_profile(&db, IDENTITY_ID, PROFILE_ID, "admitted-model").await;
    db
}

async fn create_identity_with_profile(
    db: &SqliteDb,
    identity_id: &str,
    profile_id: &str,
    model: &str,
) {
    let now = now_rfc3339();
    AgentRepo::create_identity_with_profile(
        db,
        CreateAgentIdentity {
            id: identity_id.to_owned(),
            name: format!("{identity_id}-name"),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some(ACCOUNT_ID.to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: profile_id.to_owned(),
            identity_id: identity_id.to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some(model.to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("identity and profile");
}

struct RetryRunnerSpy {
    calls: Mutex<Vec<AgentChatTurnJob>>,
    attempts: AtomicUsize,
}

impl RetryRunnerSpy {
    fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            attempts: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> Vec<AgentChatTurnJob> {
        self.calls.lock().expect("runner calls lock").clone()
    }
}

#[async_trait]
impl AgentChatTurnRunner for RetryRunnerSpy {
    async fn run_turn(
        &self,
        job: &AgentChatTurnJob,
        _cancellation: CancellationToken,
    ) -> services::Result<CompletedAgentChatTurn> {
        self.calls
            .lock()
            .expect("runner calls lock")
            .push(job.clone());
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(ServiceError::Conflict(
                "synthetic transient failure".to_owned(),
            ));
        }
        Ok(CompletedAgentChatTurn {
            identity_id: job
                .responder_identity_id
                .clone()
                .expect("admitted identity"),
            profile_id: job.profile_id.clone().expect("admitted Profile"),
            session_id: "retry-runner-session".to_owned(),
            model: Some("admitted-model".to_owned()),
            content: "retry succeeded".to_owned(),
            token_usage_json: None,
            duration_ms: 1,
            context_manifest_id: None,
            pending_interaction_id: None,
        })
    }
}

struct EmptyResponseRunner;

#[async_trait]
impl AgentChatTurnRunner for EmptyResponseRunner {
    async fn run_turn(
        &self,
        job: &AgentChatTurnJob,
        _cancellation: CancellationToken,
    ) -> services::Result<CompletedAgentChatTurn> {
        Ok(CompletedAgentChatTurn {
            identity_id: job
                .responder_identity_id
                .clone()
                .expect("admitted identity"),
            profile_id: job.profile_id.clone().expect("admitted Profile"),
            session_id: "empty-response-session".to_owned(),
            model: Some("admitted-model".to_owned()),
            content: " \n ".to_owned(),
            token_usage_json: None,
            duration_ms: 1,
            context_manifest_id: None,
            pending_interaction_id: None,
        })
    }
}

struct GenesisTransferRunner {
    db: Arc<SqliteDb>,
    chat_id: String,
    command_committed: AtomicUsize,
    provider_stopped: AtomicUsize,
}

#[async_trait]
impl AgentChatTurnRunner for GenesisTransferRunner {
    async fn run_turn(
        &self,
        _job: &AgentChatTurnJob,
        cancellation: CancellationToken,
    ) -> services::Result<CompletedAgentChatTurn> {
        MainGenesisCommandService::new(Arc::clone(&self.db))
            .start(MainGenesisStartCommandInput {
                principal: MainGenesisStartPrincipal::MainAgent {
                    identity_id: IDENTITY_ID.to_owned(),
                    scope: CanonicalScope {
                        scope_type: CanonicalScopeType::AgentChat,
                        scope_id: self.chat_id.clone(),
                        workspace_access: WorkspaceAccess::Deny,
                    },
                },
                request: MainGenesisStartRequest {
                    maturity: Some(ProductMaturity::Mvp),
                    initial_idea: None,
                    preferred_project_agent_identity_id: None,
                },
                idempotency_key: "worker-genesis-control-transfer".to_owned(),
                correlation_id: "worker-genesis-control-transfer-correlation".to_owned(),
                causation_id: None,
                causation_depth: 0,
                policy_result: "allowed".to_owned(),
                requested_permission: "propose_discovery".to_owned(),
            })
            .await?;
        self.command_committed.fetch_add(1, Ordering::SeqCst);
        cancellation.cancelled().await;
        self.provider_stopped.fetch_add(1, Ordering::SeqCst);
        Err(ServiceError::Conflict(
            "baseline provider stopped by Genesis control transfer".to_owned(),
        ))
    }
}

#[tokio::test]
async fn genesis_start_rejects_a_project_agent_display_name_before_persistence() {
    let db = database().await;
    let chats = AgentChatService::new(Arc::clone(&db));
    chats
        .set_main_binding(SetMainAgentBindingInput {
            actor_user_id: ACCOUNT_ID.to_owned(),
            account_id: ACCOUNT_ID.to_owned(),
            identity_id: IDENTITY_ID.to_owned(),
            autonomy_policy_json: "{}".to_owned(),
            tool_policy_revision: "genesis-invalid-project-agent-policy".to_owned(),
            expected_version: None,
            replacement_reason: None,
        })
        .await
        .expect("Main binding");

    let error = MainGenesisCommandService::new(Arc::clone(&db))
        .start(MainGenesisStartCommandInput {
            principal: MainGenesisStartPrincipal::User {
                user_id: ACCOUNT_ID.to_owned(),
            },
            request: MainGenesisStartRequest {
                maturity: Some(ProductMaturity::Mvp),
                initial_idea: Some("Build a local note app.".to_owned()),
                preferred_project_agent_identity_id: Some("Gate E OpenAI Project".to_owned()),
            },
            idempotency_key: "genesis-invalid-project-agent".to_owned(),
            correlation_id: "genesis-invalid-project-agent-correlation".to_owned(),
            causation_id: None,
            causation_depth: 0,
            policy_result: "allowed".to_owned(),
            requested_permission: "propose_discovery".to_owned(),
        })
        .await
        .expect_err("display name must not be accepted as an identity id");
    assert!(matches!(
        error,
        ServiceError::InvalidOperation { ref message }
            if message.contains("is not an eligible account-owned identity")
    ));

    let sessions = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM product_genesis_session")
        .fetch_one(db.pool())
        .await
        .expect("Genesis count");
    assert_eq!(sessions, 0, "invalid input must not persist Genesis state");
}

#[tokio::test]
async fn genesis_control_transfer_stops_provider_and_commits_no_baseline_response() {
    let db = database().await;
    let chats = AgentChatService::new(Arc::clone(&db));
    chats
        .set_main_binding(SetMainAgentBindingInput {
            actor_user_id: ACCOUNT_ID.to_owned(),
            account_id: ACCOUNT_ID.to_owned(),
            identity_id: IDENTITY_ID.to_owned(),
            autonomy_policy_json: "{}".to_owned(),
            tool_policy_revision: "genesis-control-transfer-policy".to_owned(),
            expected_version: None,
            replacement_reason: None,
        })
        .await
        .expect("Main binding");
    let chat = AgentChatRepo::get_main_chat(&*db, ACCOUNT_ID)
        .await
        .expect("Main Chat lookup")
        .expect("Main Chat");
    let admitted = chats
        .send_message(SendAgentChatMessageInput {
            actor_user_id: ACCOUNT_ID.to_owned(),
            chat_id: chat.id.clone(),
            content: "Start a new Project for calm incident reviews.".to_owned(),
            dedupe_key: Some("worker-genesis-user-message".to_owned()),
        })
        .await
        .expect("baseline semantic-start turn");
    let runner = Arc::new(GenesisTransferRunner {
        db: Arc::clone(&db),
        chat_id: chat.id.clone(),
        command_committed: AtomicUsize::new(0),
        provider_stopped: AtomicUsize::new(0),
    });
    let worker = AgentChatTurnWorker::with_runner(
        Arc::clone(&db),
        runner.clone() as Arc<dyn AgentChatTurnRunner>,
    );
    assert_eq!(worker.run_once().await.expect("control-transfer run"), 1);
    assert_eq!(runner.command_committed.load(Ordering::SeqCst), 1);
    assert_eq!(runner.provider_stopped.load(Ordering::SeqCst), 1);

    let source = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*db, &admitted.turn_job.id)
        .await
        .expect("source turn lookup")
        .expect("source turn");
    assert_eq!(source.status, AgentChatTurnState::Succeeded);
    assert!(source.response_message_id.is_none());
    let messages: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_message WHERE chat_id = ?")
            .bind(&chat.id)
            .fetch_one(db.pool())
            .await
            .expect("message count");
    assert_eq!(
        messages, 1,
        "only the original visible user message remains"
    );
    let continuation: (String, String) = sqlx::query_as(
        "SELECT status, operating_skill_revision_id
         FROM agent_chat_turn_job WHERE id <> ? AND triggering_message_id = ?",
    )
    .bind(&source.id)
    .bind(&admitted.message.id)
    .fetch_one(db.pool())
    .await
    .expect("queued discovery continuation");
    assert_eq!(continuation.0, "queued");
    assert!(continuation
        .1
        .starts_with("forge.main.project-discovery/v2@"));
}

fn frozen_provenance(job: &AgentChatTurnJob) -> serde_json::Value {
    json!({
        "responder_identity_id": job.responder_identity_id,
        "profile_id": job.profile_id,
        "responder_binding_id": job.responder_binding_id,
        "responder_binding_version": job.responder_binding_version,
        "responder_identity_version": job.responder_identity_version,
        "profile_version": job.profile_version,
        "operating_skill_revision_id": job.operating_skill_revision_id,
        "policy_revision": job.policy_revision,
        "policy_digest": job.policy_digest,
        "permission_policy_digest": job.permission_policy_digest,
        "tool_policy_digest": job.tool_policy_digest,
        "admission_digest": job.admission_digest,
        "canonical_scope_type": job.canonical_scope_type,
        "canonical_scope_id": job.canonical_scope_id,
        "canonical_scope_provenance_json": job.canonical_scope_provenance_json,
    })
}

#[tokio::test]
async fn empty_provider_output_records_a_specific_visible_failure() {
    let db = database().await;
    let chats = AgentChatService::new(Arc::clone(&db));
    chats
        .set_main_binding(SetMainAgentBindingInput {
            actor_user_id: ACCOUNT_ID.to_owned(),
            account_id: ACCOUNT_ID.to_owned(),
            identity_id: IDENTITY_ID.to_owned(),
            autonomy_policy_json: "{}".to_owned(),
            tool_policy_revision: "empty-response-policy".to_owned(),
            expected_version: None,
            replacement_reason: None,
        })
        .await
        .expect("Main binding");
    let chat = AgentChatRepo::get_main_chat(&*db, ACCOUNT_ID)
        .await
        .expect("Main Chat lookup")
        .expect("Main Chat");
    let admitted = chats
        .send_message(SendAgentChatMessageInput {
            actor_user_id: ACCOUNT_ID.to_owned(),
            chat_id: chat.id,
            content: "Return a response".to_owned(),
            dedupe_key: Some("empty-response-admission".to_owned()),
        })
        .await
        .expect("turn admission")
        .turn_job;

    let worker = AgentChatTurnWorker::with_runner(
        Arc::clone(&db),
        Arc::new(EmptyResponseRunner) as Arc<dyn AgentChatTurnRunner>,
    );
    assert_eq!(worker.run_once().await.expect("worker run"), 1);

    let failed_attempt = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*db, &admitted.id)
        .await
        .expect("turn lookup")
        .expect("turn");
    assert_eq!(failed_attempt.status, AgentChatTurnState::RetryWait);
    assert_eq!(failed_attempt.error_code.as_deref(), Some("empty_response"));
    assert_eq!(
        failed_attempt.error_message.as_deref(),
        Some("Agent returned no text response")
    );
    assert_eq!(failed_attempt.response_message_id, None);
}

#[tokio::test]
async fn retry_reuses_frozen_runner_job_after_profile_and_binding_edits() {
    let db = database().await;
    let chats = AgentChatService::new(Arc::clone(&db));
    let binding = chats
        .set_main_binding(SetMainAgentBindingInput {
            actor_user_id: ACCOUNT_ID.to_owned(),
            account_id: ACCOUNT_ID.to_owned(),
            identity_id: IDENTITY_ID.to_owned(),
            autonomy_policy_json: "{}".to_owned(),
            tool_policy_revision: "admitted-tool-policy".to_owned(),
            expected_version: None,
            replacement_reason: None,
        })
        .await
        .expect("Main binding");
    let chat = AgentChatRepo::get_main_chat(&*db, ACCOUNT_ID)
        .await
        .expect("Main Chat lookup")
        .expect("Main Chat");
    let admitted = chats
        .send_message(SendAgentChatMessageInput {
            actor_user_id: ACCOUNT_ID.to_owned(),
            chat_id: chat.id.clone(),
            content: "admit once, retry later".to_owned(),
            dedupe_key: Some("worker-retry-admission".to_owned()),
        })
        .await
        .expect("turn admission")
        .turn_job;

    let spy = Arc::new(RetryRunnerSpy::new());
    let worker = AgentChatTurnWorker::with_runner(
        Arc::clone(&db),
        spy.clone() as Arc<dyn AgentChatTurnRunner>,
    );
    assert_eq!(worker.run_once().await.expect("first worker run"), 1);
    let first_attempt = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*db, &admitted.id)
        .await
        .expect("first retry lookup")
        .expect("first retry job");
    assert_eq!(first_attempt.status, AgentChatTurnState::RetryWait);

    // A direct Profile edit selects a new current revision after admission.
    let current_identity = AgentRepo::get_by_id(&*db, IDENTITY_ID)
        .await
        .expect("identity lookup")
        .expect("identity");
    let edited_profile_id = new_uuid_v4();
    let now = now_rfc3339();
    AgentProfileRepo::create_and_select_profile(
        &*db,
        CreateAgentProfile {
            id: edited_profile_id.clone(),
            identity_id: IDENTITY_ID.to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("current-edited-model".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            tool_policy_json: "{\"edited\":true}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        SelectAgentProfile {
            identity_id: IDENTITY_ID.to_owned(),
            profile_id: edited_profile_id.clone(),
            expected_version: current_identity.version,
            updated_at: now.clone(),
        },
    )
    .await
    .expect("Profile edit");

    // Replace the live binding with another owned responder after the first
    // invocation. The retry must still pass the old admitted job to the
    // runner, rather than resolving this replacement at execution time.
    let replacement_identity_id = "worker-retry-replacement";
    let replacement_profile_id = "worker-retry-replacement-profile";
    create_identity_with_profile(
        &db,
        replacement_identity_id,
        replacement_profile_id,
        "replacement-model",
    )
    .await;
    chats
        .set_main_binding(SetMainAgentBindingInput {
            actor_user_id: ACCOUNT_ID.to_owned(),
            account_id: ACCOUNT_ID.to_owned(),
            identity_id: replacement_identity_id.to_owned(),
            autonomy_policy_json: "{\"replacement\":true}".to_owned(),
            tool_policy_revision: "replacement-tool-policy".to_owned(),
            expected_version: Some(binding.version),
            replacement_reason: Some("retry provenance characterization".to_owned()),
        })
        .await
        .expect("binding replacement");

    let current_binding = AccountMainAgentBindingRepo::get_active_main_binding(&*db, ACCOUNT_ID)
        .await
        .expect("current binding lookup")
        .expect("current binding");
    assert_ne!(current_binding.identity_id, IDENTITY_ID);
    let current_identity = AgentRepo::get_by_id(&*db, replacement_identity_id)
        .await
        .expect("replacement identity lookup")
        .expect("replacement identity");
    assert_ne!(current_identity.profile_id, PROFILE_ID);

    // Make the finite RetryWait cooldown ready without changing the admitted
    // job's immutable provenance columns.
    let now = now_rfc3339();
    sqlx::query(
        "UPDATE agent_chat_turn_job
         SET next_attempt_at = ?, version = version + 1, updated_at = ?
         WHERE id = ? AND status = 'retry_wait'",
    )
    .bind("1970-01-01T00:00:00Z")
    .bind(&now)
    .bind(&admitted.id)
    .execute(db.pool())
    .await
    .expect("retry cooldown");

    assert_eq!(worker.run_once().await.expect("retry worker run"), 1);
    let calls = spy.calls();
    assert_eq!(calls.len(), 2, "runner sees the original and retry attempt");
    assert_eq!(frozen_provenance(&calls[0]), frozen_provenance(&calls[1]));
    assert_eq!(calls[0].responder_identity_id.as_deref(), Some(IDENTITY_ID));
    assert_eq!(calls[0].profile_id.as_deref(), Some(PROFILE_ID));
    assert!(calls[0]
        .admission_digest
        .as_deref()
        .is_some_and(|digest| !digest.is_empty()));
    assert!(calls[0]
        .canonical_scope_provenance_json
        .as_deref()
        .is_some_and(|provenance| !provenance.is_empty()));

    let completed = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*db, &admitted.id)
        .await
        .expect("completed lookup")
        .expect("completed turn");
    assert_eq!(completed.status, AgentChatTurnState::Succeeded);
    assert_eq!(
        completed.responder_identity_id.as_deref(),
        Some(IDENTITY_ID)
    );
    assert_eq!(completed.profile_id.as_deref(), Some(PROFILE_ID));
}

struct TypedFailureRunner {
    failure: api_types::TurnFailure,
    calls: AtomicUsize,
    admission_calls: AtomicUsize,
    pre_provider: bool,
}

#[async_trait]
impl AgentChatTurnRunner for TypedFailureRunner {
    async fn validate_provider_availability(&self, _: &AgentChatTurnJob) -> services::Result<()> {
        self.admission_calls.fetch_add(1, Ordering::SeqCst);
        if self.pre_provider {
            Err(ServiceError::Conflict("admission unavailable".to_owned()))
        } else {
            Ok(())
        }
    }

    async fn run_turn(
        &self,
        _: &AgentChatTurnJob,
        _: CancellationToken,
    ) -> services::Result<CompletedAgentChatTurn> {
        unreachable!("typed outcome is used")
    }

    async fn run_turn_with_usage(
        &self,
        job: &AgentChatTurnJob,
        _: CancellationToken,
    ) -> services::AgentChatTurnRunOutcome {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call > 0 && matches!(self.failure, api_types::TurnFailure::UsageLimit { .. }) {
            services::AgentChatTurnRunOutcome::Completed {
                turn: CompletedAgentChatTurn {
                    identity_id: job.responder_identity_id.clone().unwrap(),
                    profile_id: job.profile_id.clone().unwrap(),
                    session_id: "typed-failure-session".into(),
                    model: None,
                    content: "resumed after reset".into(),
                    token_usage_json: None,
                    duration_ms: 1,
                    context_manifest_id: None,
                    pending_interaction_id: None,
                },
                usage_reports: Vec::new(),
            }
        } else {
            services::AgentChatTurnRunOutcome::Failed {
                failure: self.failure.clone(),
                error: ServiceError::Conflict(
                    "detail contains config usage limit credential but is not classified"
                        .to_owned(),
                ),
                usage_reports: Vec::new(),
            }
        }
    }
}

async fn admit_failure_test(db: &Arc<SqliteDb>) -> AgentChatTurnJob {
    let chats = AgentChatService::new(db.clone());
    chats
        .set_main_binding(SetMainAgentBindingInput {
            actor_user_id: ACCOUNT_ID.into(),
            account_id: ACCOUNT_ID.into(),
            identity_id: IDENTITY_ID.into(),
            autonomy_policy_json: "{}".into(),
            tool_policy_revision: "typed-failure-policy".into(),
            expected_version: None,
            replacement_reason: None,
        })
        .await
        .unwrap();
    let chat = AgentChatRepo::get_main_chat(&**db, ACCOUNT_ID)
        .await
        .unwrap()
        .unwrap();
    chats
        .send_message(SendAgentChatMessageInput {
            actor_user_id: ACCOUNT_ID.into(),
            chat_id: chat.id,
            content: "one request".into(),
            dedupe_key: Some("typed-failure-test".into()),
        })
        .await
        .unwrap()
        .turn_job
}

fn failure_runner(failure: api_types::TurnFailure, pre_provider: bool) -> Arc<TypedFailureRunner> {
    Arc::new(TypedFailureRunner {
        failure,
        calls: AtomicUsize::new(0),
        admission_calls: AtomicUsize::new(0),
        pre_provider,
    })
}

async fn current_turn(db: &SqliteDb, id: &str) -> AgentChatTurnJob {
    AgentChatTurnJobRepo::get_agent_chat_turn_job(db, id)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn usage_limit_refunds_attempt_and_resumes_after_reset_with_new_invocation() {
    let db = database().await;
    let job = admit_failure_test(&db).await;
    let reset = chrono::DateTime::from_timestamp_millis(
        (chrono::Utc::now() + chrono::Duration::hours(2)).timestamp_millis(),
    )
    .unwrap();
    let runner = failure_runner(
        api_types::TurnFailure::UsageLimit {
            resets_at: Some(reset.timestamp_millis() as u64),
        },
        false,
    );
    let worker = AgentChatTurnWorker::with_runner(db.clone(), runner.clone());
    assert_eq!(worker.run_once().await.unwrap(), 1);
    let deferred = current_turn(&db, &job.id).await;
    assert_eq!(deferred.status, AgentChatTurnState::RetryWait);
    assert_eq!(deferred.attempt_count, 0);
    assert_eq!(
        deferred.retry_decision,
        Some(api_types::TurnRetryDecision::Defer)
    );
    assert_eq!(deferred.invocation_count, 1);
    assert_eq!(worker.run_once().await.unwrap(), 0);
    let due =
        chrono::DateTime::parse_from_rfc3339(deferred.next_attempt_at.as_deref().unwrap()).unwrap();
    assert!(due >= reset);
    assert_eq!(due.timestamp_millis(), reset.timestamp_millis());
    assert_eq!(
        worker
            .run_once_at(reset - chrono::Duration::milliseconds(1))
            .await
            .unwrap(),
        0
    );
    assert_eq!(worker.run_once_at(reset).await.unwrap(), 1);
    let succeeded = current_turn(&db, &job.id).await;
    assert_eq!(succeeded.status, AgentChatTurnState::Succeeded);
    assert_eq!(succeeded.attempt_count, 1);
    assert_eq!(succeeded.invocation_count, 2);
    assert_eq!(runner.calls.load(Ordering::SeqCst), 2);
    let invocations: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM usage_invocation WHERE source_id = ?")
            .bind(&job.id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(
        invocations, 2,
        "refunded budget must not reuse terminal accounting identity"
    );
}

#[tokio::test]
async fn pre_provider_admission_refunds_attempt_and_stops_at_separate_cap() {
    let db = database().await;
    let job = admit_failure_test(&db).await;
    let runner = failure_runner(api_types::TurnFailure::Unclassified, true);
    let worker = AgentChatTurnWorker::with_runner(db.clone(), runner.clone());
    for count in 1..=3 {
        assert_eq!(worker.run_once().await.unwrap(), 1);
        let current = current_turn(&db, &job.id).await;
        assert_eq!(current.attempt_count, 0);
        assert_eq!(current.pre_provider_failure_count, count);
        assert_eq!(
            current.status,
            if count < 3 {
                AgentChatTurnState::RetryWait
            } else {
                AgentChatTurnState::Failed
            }
        );
        if count < 3 {
            assert_eq!(worker.run_once().await.unwrap(), 0);
            sqlx::query("UPDATE agent_chat_turn_job SET next_attempt_at = '2000-01-01T00:00:00Z' WHERE id = ?").bind(&job.id).execute(db.pool()).await.unwrap();
        }
    }
    assert_eq!(worker.run_once().await.unwrap(), 0);
    assert_eq!(runner.calls.load(Ordering::SeqCst), 0);
    assert_eq!(runner.admission_calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn expired_lease_persists_its_current_failure_decision() {
    let db = database().await;
    let job = admit_failure_test(&db).await;
    let runner = failure_runner(api_types::TurnFailure::Configuration, false);
    // The lease refunds are spent, so this expiry counts as the final attempt.
    sqlx::query("UPDATE agent_chat_turn_job SET status = 'leased', lease_owner = 'expired-worker', leased_until = '2000-01-01T00:00:00Z', attempt_count = 3, invocation_count = 3, pre_provider_failure_count = 2, lease_refund_count = ?, failure_class_json = '{\"kind\":\"configuration\"}', retry_decision = 'retry' WHERE id = ?")
        .bind(services::agent_chat_turn_policy::MAX_LEASE_REFUNDS).bind(&job.id).execute(db.pool()).await.unwrap();
    let worker = AgentChatTurnWorker::with_runner(db.clone(), runner.clone());
    assert_eq!(worker.run_once().await.unwrap(), 0);
    let recovered = current_turn(&db, &job.id).await;
    assert_eq!(recovered.status, AgentChatTurnState::Failed);
    assert_eq!(
        recovered.failure_class,
        Some(api_types::TurnFailure::Unclassified)
    );
    assert_eq!(
        recovered.retry_decision,
        Some(api_types::TurnRetryDecision::Fail)
    );
    assert_eq!(recovered.pre_provider_failure_count, 2);
    let refunds: i64 =
        sqlx::query_scalar("SELECT lease_refund_count FROM agent_chat_turn_job WHERE id = ?")
            .bind(&job.id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(
        refunds,
        services::agent_chat_turn_policy::MAX_LEASE_REFUNDS,
        "a counted expiry is not a refund"
    );
    assert!(recovered.retry_action().is_some());
    assert_eq!(runner.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn manual_retry_admits_current_profile_is_versioned_idempotent_and_resolves_incident() {
    use api_types::TurnFailure;
    for failure in [
        TurnFailure::Configuration,
        TurnFailure::Authority,
        TurnFailure::ProviderSchema,
        TurnFailure::ProviderAuth,
        // Stored by turns that failed before typed provider failures.
        TurnFailure::ProviderRejected {
            retryable: false,
            retry_after: None,
        },
        TurnFailure::Unclassified,
    ] {
        let db = database().await;
        let job = admit_failure_test(&db).await;
        let runner = failure_runner(failure.clone(), false);
        let worker = AgentChatTurnWorker::with_runner(db.clone(), runner);
        assert_eq!(worker.run_once().await.unwrap(), 1);
        if !failure.requires_attention() {
            // Finish the ordinary attempt budget first.
            for _ in 0..2 {
                sqlx::query("UPDATE agent_chat_turn_job SET next_attempt_at = '2000-01-01T00:00:00Z' WHERE id = ?").bind(&job.id).execute(db.pool()).await.unwrap();
                worker.run_once().await.unwrap();
            }
        }
        let failed = current_turn(&db, &job.id).await;
        assert_eq!(failed.status, AgentChatTurnState::Failed);
        assert!(failed.retry_action().is_some());
        let attention = services::AttentionService::new(db.clone());
        attention.project_once(100).await.unwrap();
        attention.project_once(100).await.unwrap();
        let items: Vec<(String, String, String)> = sqlx::query_as("SELECT summary, recommended_action, details_json FROM attention_projection WHERE source_event_id IN (SELECT id FROM domain_event WHERE entity_id = ? AND event_type = 'agent_chat.turn.failed')")
            .bind(&job.id).fetch_all(db.pool()).await.unwrap();
        assert_eq!(
            items.len(),
            usize::from(failure.requires_attention()),
            "only deterministic failures raise an incident"
        );
        if failure.requires_attention() {
            assert!(items[0].0.contains(failure.code()));
            assert_eq!(items[0].1, "retry_turn");
            let details: serde_json::Value = serde_json::from_str(&items[0].2).unwrap();
            assert_eq!(details["retry_action"]["kind"], "retry_turn");
            sqlx::query("UPDATE attention_projection SET snoozed_until = '2099-01-01T00:00:00Z' WHERE source_event_id IN (SELECT id FROM domain_event WHERE entity_id = ?)")
                .bind(&job.id).execute(db.pool()).await.unwrap();
        }
        let chats = AgentChatService::new(db.clone());
        let input = services::RetryAgentChatTurnInput {
            actor_user_id: ACCOUNT_ID.into(),
            chat_id: job.chat_id.clone(),
            turn_job_id: job.id.clone(),
            expected_version: failed.version,
            idempotency_key: "manual-fix".into(),
        };
        let stale = services::RetryAgentChatTurnInput {
            expected_version: failed.version - 1,
            ..input.clone()
        };
        assert!(matches!(
            chats.retry_turn(stale).await,
            Err(ServiceError::Db(db::DbError::VersionConflict))
        ));
        let agent = AgentRepo::get_by_id(&*db, IDENTITY_ID)
            .await
            .unwrap()
            .unwrap();
        let updated = AgentRepo::update(
            &*db,
            db::UpdateAgent {
                id: agent.id,
                expected_version: agent.version,
                model: Some(Some("corrected-model".into())),
                name: None,
                description: None,
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: None,
                config_json: None,
                daemon_id: None,
                max_concurrent_tasks: None,
                heartbeat_interval_seconds: None,
                max_missed_heartbeats: None,
                status: None,
                last_heartbeat_at: None,
                is_default: None,
                paused: None,
                updated_at: db::now_rfc3339(),
            },
        )
        .await
        .unwrap();
        assert_ne!(Some(updated.profile_id.as_str()), job.profile_id.as_deref());
        let retried = chats.retry_turn(input.clone()).await.unwrap();
        assert_ne!(retried.id, job.id);
        assert_eq!(retried.status, AgentChatTurnState::Queued);
        assert_eq!(retried.attempt_count, 0);
        assert_eq!(retried.triggering_message_id, job.triggering_message_id);
        assert_eq!(
            retried.profile_id.as_deref(),
            Some(updated.profile_id.as_str())
        );
        let incident: Option<(String, Option<String>)> = sqlx::query_as(
            "SELECT status, snoozed_until FROM attention_projection WHERE details_json LIKE ?",
        )
        .bind(format!("%{}%", job.id))
        .fetch_optional(db.pool())
        .await
        .unwrap();
        assert_eq!(incident.is_some(), failure.requires_attention());
        if let Some((status, snoozed)) = incident {
            assert_eq!(status, "resolved");
            assert!(snoozed.is_none());
        }
        assert_eq!(chats.retry_turn(input.clone()).await.unwrap(), retried);
        let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_event WHERE entity_id = ? AND event_type = 'agent_chat.turn.retried'").bind(&retried.id).fetch_one(db.pool()).await.unwrap();
        assert_eq!(events, 1);
        assert_eq!(
            AgentChatTurnJobRepo::list_agent_chat_turn_jobs(&*db, &job.chat_id)
                .await
                .unwrap()
                .len(),
            2
        );
        let source = current_turn(&db, &job.id).await;
        let fresh_key = services::RetryAgentChatTurnInput {
            expected_version: source.version,
            idempotency_key: "another-fix".into(),
            ..input.clone()
        };
        assert!(matches!(
            chats.retry_turn(fresh_key.clone()).await,
            Err(ServiceError::Db(db::DbError::TurnNotRetryable))
        ));
        sqlx::query("UPDATE agent_chat_turn_job SET status = 'succeeded' WHERE id = ?")
            .bind(&retried.id)
            .execute(db.pool())
            .await
            .unwrap();
        assert!(matches!(
            chats.retry_turn(fresh_key).await,
            Err(ServiceError::Db(db::DbError::TurnNotRetryable))
        ));
    }
}

#[tokio::test]
async fn newer_message_turn_supersedes_retry_and_resolves_snoozed_incident() {
    for topic_divider in [false, true] {
        let db = database().await;
        let job = admit_failure_test(&db).await;
        let worker = AgentChatTurnWorker::with_runner(
            db.clone(),
            failure_runner(api_types::TurnFailure::Configuration, false),
        );
        worker.run_once().await.unwrap();
        let attention = services::AttentionService::new(db.clone());
        attention.project_once(100).await.unwrap();
        attention.project_once(100).await.unwrap();
        sqlx::query(
        "UPDATE attention_projection SET status = 'open', snoozed_until = '2099-01-01T00:00:00Z'",
    )
    .execute(db.pool())
    .await
    .unwrap();
        if topic_divider {
            services::MainChatTopicService::new(
                db.clone(),
                Arc::new(AgentChatService::new(db.clone())),
            )
            .start_topic(services::StartMainChatTopicInput {
                actor_user_id: ACCOUNT_ID.into(),
                chat_id: job.chat_id.clone(),
                label: Some("new topic".into()),
                summary: None,
            })
            .await
            .unwrap();
            // This fixture has a fake turn runner. Complete its durable intent
            // through the topic transaction before checking attention supersession.
            let intent: (String, String, String) = sqlx::query_as(
                "SELECT id, label, created_at FROM agent_chat_topic_rotation WHERE chat_id = ?",
            )
            .bind(&job.chat_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
            sqlx::query("UPDATE agent_chat_topic_rotation SET owner_token = 'attention-fixture' WHERE chat_id = ?")
                .bind(&job.chat_id).execute(db.pool()).await.unwrap();
            db::AgentChatTopicTransactionRepo::rotate_agent_chat_topic(
                &*db,
                db::RotateAgentChatTopic {
                    runtime_session_id: None,
                    rotation_owner: Some("attention-fixture".into()),
                    topic: db::CreateAgentChatTopic {
                        id: intent.0.clone(),
                        chat_id: job.chat_id.clone(),
                        label: intent.1.clone(),
                        summary: None,
                        principal_type: "system".into(),
                        principal_id: None,
                        created_at: intent.2.clone(),
                    },
                    divider_message: db::topic_divider_message(
                        format!("divider:{}", intent.0),
                        job.chat_id.clone(),
                        &intent.1,
                        "attention-fixture".into(),
                        intent.2,
                    ),
                },
            )
            .await
            .unwrap()
            .unwrap();
        } else {
            AgentChatService::new(db.clone())
                .send_message(SendAgentChatMessageInput {
                    actor_user_id: ACCOUNT_ID.into(),
                    chat_id: job.chat_id.clone(),
                    content: "new topic".into(),
                    dedupe_key: Some("new-topic".into()),
                })
                .await
                .unwrap();
        }
        assert!(current_turn(&db, &job.id).await.retry_action().is_none());
        attention.project_once(100).await.unwrap();
        let (status, snoozed): (String, Option<String>) = sqlx::query_as(
            "SELECT status, snoozed_until FROM attention_projection WHERE details_json LIKE ?",
        )
        .bind(format!("%{}%", job.id))
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(status, "resolved");
        assert!(snoozed.is_none());
    }
}

#[tokio::test]
async fn usage_limit_elapsed_budget_fails_before_another_provider_call() {
    let db = database().await;
    let job = admit_failure_test(&db).await;
    let runner = failure_runner(
        api_types::TurnFailure::UsageLimit { resets_at: None },
        false,
    );
    let worker = AgentChatTurnWorker::with_runner(db.clone(), runner.clone());
    worker.run_once().await.unwrap();
    sqlx::query("UPDATE agent_chat_turn_job SET usage_limit_first_deferred_at = ?, next_attempt_at = '2000-01-01T00:00:00Z' WHERE id = ?")
        .bind((chrono::Utc::now() - chrono::Duration::hours(24)).to_rfc3339()).bind(&job.id).execute(db.pool()).await.unwrap();
    worker.run_once().await.unwrap();
    let failed = current_turn(&db, &job.id).await;
    assert_eq!(failed.status, AgentChatTurnState::Failed);
    assert_eq!(failed.attempt_count, 0);
    assert_eq!(
        failed.failure_class,
        Some(api_types::TurnFailure::UsageLimit { resets_at: None })
    );
    assert!(failed.retry_action().is_some());
    assert_eq!(runner.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn orphaned_parked_turn_records_typed_terminal_decision() {
    let db = database().await;
    let job = admit_failure_test(&db).await;
    sqlx::query("UPDATE agent_chat_turn_job SET status = 'awaiting_input', pending_interaction_id = NULL WHERE id = ?")
        .bind(&job.id).execute(db.pool()).await.unwrap();
    let runner = failure_runner(api_types::TurnFailure::Unclassified, false);
    let worker = AgentChatTurnWorker::with_runner(db.clone(), runner.clone());
    assert_eq!(worker.run_once().await.unwrap(), 0);
    let failed = current_turn(&db, &job.id).await;
    assert_eq!(failed.status, AgentChatTurnState::Failed);
    assert_eq!(failed.error_code.as_deref(), Some("interaction_orphaned"));
    assert_eq!(
        failed.failure_class,
        Some(api_types::TurnFailure::Unclassified)
    );
    assert_eq!(
        failed.retry_decision,
        Some(api_types::TurnRetryDecision::Fail)
    );
    assert!(failed.retry_action().is_some());
    assert_eq!(runner.calls.load(Ordering::SeqCst), 0);
}
