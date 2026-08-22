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
