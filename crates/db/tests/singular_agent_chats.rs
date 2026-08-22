use db::{
    create_sqlite_pool, run_migrations, AdmitAgentChatTurn, AdmitAgentHandoff,
    AgentChatMessageAuthorType, AgentChatMessageStatus, AgentChatRepo, AgentChatTransactionRepo,
    AgentChatTurnJobRepo, AgentChatTurnState, AgentRepo, AgentStatus, CancelAgentChatTurn,
    CreateAgentChatMessage, CreateAgentChatTurnJob, CreateAgentHandoff, CreateAgentIdentity,
    CreateAgentProfile, CreateProject, DbError, DomainEventRepo, FailAgentChatTurn,
    ProjectAgentBindingRepo, ProjectRepo, ReplaceProjectAgentBinding, SqliteDb,
    UpdateAgentChatTurnJob, User, UserRepo,
};
use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

async fn database() -> SqliteDb {
    database_url("sqlite::memory:").await
}

async fn database_url(url: &str) -> SqliteDb {
    let pool = create_sqlite_pool(url).await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    SqliteDb::new(pool)
}

async fn fixture() -> (SqliteDb, String, String, String, String) {
    fixture_on(database().await).await
}

async fn fixture_on(db: SqliteDb) -> (SqliteDb, String, String, String, String) {
    let now = "2026-08-13T00:00:00.000Z".to_owned();
    let account_id = "chat-account".to_owned();
    UserRepo::create_user(
        &db,
        &User {
            id: account_id.clone(),
            email: "chat-account@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Chat Account".to_owned()),
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("account creates");

    let project_id = "chat-project".to_owned();
    ProjectRepo::create(
        &db,
        CreateProject {
            id: project_id.clone(),
            name: "Chat Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(account_id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");

    let identity_id = "chat-agent".to_owned();
    let profile_id = "chat-profile".to_owned();
    AgentRepo::create_identity_with_profile(
        &db,
        CreateAgentIdentity {
            id: identity_id.clone(),
            name: "Chat Agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some(account_id.clone()),
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: profile_id.clone(),
            identity_id: identity_id.clone(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test-model".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("identity creates");

    let main = AgentChatRepo::get_main_chat(&db, &account_id)
        .await
        .expect("main chat lookup")
        .expect("main chat exists");
    AgentChatRepo::update_agent_chat(
        &db,
        db::UpdateAgentChat {
            id: main.id,
            expected_version: 1,
            status: Some("ready".to_owned()),
            instruction_revision: None,
            updated_at: now.clone(),
        },
    )
    .await
    .expect("main chat ready");

    let project_binding = ProjectAgentBindingRepo::get_active_project_binding(&db, &project_id)
        .await
        .expect("project setup binding lookup")
        .expect("project setup binding exists");
    ProjectAgentBindingRepo::replace_project_binding(
        &db,
        ReplaceProjectAgentBinding {
            project_id: project_id.clone(),
            expected_version: project_binding.version,
            replacement: db::CreateProjectAgentBinding {
                id: "chat-project-binding".to_owned(),
                project_id: project_id.clone(),
                identity_id: Some(identity_id.clone()),
                profile_id: Some(profile_id.clone()),
                state: "active".to_owned(),
                autonomy_policy_json: "{}".to_owned(),
                permission_ceiling_json: "{}".to_owned(),
                subscriptions_json: "[]".to_owned(),
                wake_budget: 1,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            replacement_reason: Some("test selection".to_owned()),
        },
    )
    .await
    .expect("project binding selected");
    let project_chat = AgentChatRepo::get_project_chat(&db, &project_id)
        .await
        .expect("project chat lookup")
        .expect("project chat exists");
    AgentChatRepo::update_agent_chat(
        &db,
        db::UpdateAgentChat {
            id: project_chat.id.clone(),
            expected_version: 1,
            status: Some("ready".to_owned()),
            instruction_revision: None,
            updated_at: now,
        },
    )
    .await
    .expect("project chat ready");

    (db, account_id, project_id, identity_id, profile_id)
}

fn user_message(id: &str, chat_id: &str, now: &str) -> CreateAgentChatMessage {
    CreateAgentChatMessage {
        id: id.to_owned(),
        chat_id: chat_id.to_owned(),
        sequence: 999,
        author_type: AgentChatMessageAuthorType::User,
        author_id: Some("chat-account".to_owned()),
        content: format!("message {id}"),
        content_guard_json: "{}".to_owned(),
        sensitivity: "internal".to_owned(),
        status: AgentChatMessageStatus::Complete,
        outcome: None,
        model: None,
        profile_id: None,
        session_id: None,
        context_manifest_id: None,
        token_usage_json: None,
        duration_ms: None,
        error: None,
        correlation_id: format!("corr-{id}"),
        causation_id: None,
        handoff_id: None,
        source_type: "native".to_owned(),
        source_id: None,
        source_message_id: None,
        source_room_id: None,
        source_conversation_id: None,
        source_sequence: None,
        source_metadata_json: "{}".to_owned(),
        created_at: now.to_owned(),
    }
}

fn turn(
    id: &str,
    chat_id: &str,
    message_id: &str,
    identity_id: &str,
    profile_id: &str,
    dedupe: &str,
    now: &str,
) -> CreateAgentChatTurnJob {
    CreateAgentChatTurnJob {
        id: id.to_owned(),
        chat_id: chat_id.to_owned(),
        triggering_message_id: message_id.to_owned(),
        responder_identity_id: identity_id.to_owned(),
        profile_id: profile_id.to_owned(),
        responder_binding_id: None,
        responder_binding_version: None,
        responder_identity_version: None,
        profile_version: None,
        operating_skill_revision_id: None,
        policy_revision: None,
        policy_digest: None,
        permission_policy_digest: None,
        tool_policy_digest: None,
        admission_digest: None,
        canonical_scope_provenance_json: None,
        canonical_scope_type: "agent_chat".to_owned(),
        canonical_scope_id: chat_id.to_owned(),
        dedupe_key: dedupe.to_owned(),
        max_attempts: 3,
        correlation_id: format!("corr-{id}"),
        causation_id: None,
        causation_depth: 0,
        created_at: now.to_owned(),
        updated_at: now.to_owned(),
    }
}

fn unique_sqlite_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "forge-agent-chat-failure-{}-{nanos}.db",
        std::process::id()
    ))
}

fn sqlite_url(path: &Path) -> String {
    format!("sqlite://{}", path.display())
}

#[tokio::test]
async fn owner_triggers_create_setup_chat_and_binding() {
    let (db, account_id, project_id, _, _) = fixture().await;
    let main_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_chat WHERE kind = 'account_main' AND account_id = ?",
    )
    .bind(&account_id)
    .fetch_one(db.pool())
    .await
    .expect("main count");
    let project_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_chat WHERE kind = 'project' AND project_id = ?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .expect("project count");
    assert_eq!(main_count, 1);
    assert_eq!(project_count, 1);
    let current_binding = ProjectAgentBindingRepo::get_active_project_binding(&db, &project_id)
        .await
        .expect("binding state")
        .expect("current binding");
    assert_eq!(current_binding.state, "active");
}

#[tokio::test]
async fn project_creation_with_selection_is_ready_and_bound_once() {
    let (db, account_id, _, identity_id, profile_id) = fixture().await;
    let now = "2026-08-13T00:00:10.000Z".to_owned();
    let project_id = "selected-project".to_owned();
    ProjectRepo::create_with_agent_binding(
        &db,
        CreateProject {
            id: project_id.clone(),
            name: "Selected Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(account_id),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        Some(identity_id.clone()),
        Some(profile_id.clone()),
    )
    .await
    .expect("selected project creates");

    let chat = AgentChatRepo::get_project_chat(&db, &project_id)
        .await
        .expect("selected project chat lookup")
        .expect("selected project chat exists");
    assert_eq!(chat.status, "ready");
    assert_eq!(chat.account_id, None);
    let binding = ProjectAgentBindingRepo::get_active_project_binding(&db, &project_id)
        .await
        .expect("selected project binding lookup")
        .expect("selected project binding exists");
    assert_eq!(binding.state, "active");
    assert_eq!(binding.identity_id.as_deref(), Some(identity_id.as_str()));
    assert_eq!(binding.profile_id.as_deref(), Some(profile_id.as_str()));
    let ceiling: serde_json::Value = serde_json::from_str(&binding.permission_ceiling_json)
        .expect("default Project Agent permission ceiling is JSON");
    let allowed = ceiling["allowed"]
        .as_array()
        .expect("default Project Agent ceiling has an allowed list");
    assert!(allowed.iter().any(|value| value == "propose_task"));
    assert!(!allowed.iter().any(|value| value == "task_write"));
    let current_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_agent_binding WHERE project_id = ? AND state IN ('active', 'agent_setup_required')",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .expect("current binding count");
    assert_eq!(current_count, 1);
}

#[tokio::test]
async fn turn_admission_is_deduplicated_and_sequences_are_atomic() {
    let (db, account_id, _, identity_id, profile_id) = fixture().await;
    let chat = AgentChatRepo::get_main_chat(&db, &account_id)
        .await
        .expect("chat lookup")
        .expect("chat");
    let now = "2026-08-13T00:00:01.000Z";
    let input = AdmitAgentChatTurn {
        message: user_message("turn-message-1", &chat.id, now),
        turn: turn(
            "turn-job-1",
            &chat.id,
            "turn-message-1",
            &identity_id,
            &profile_id,
            "turn-dedupe-1",
            now,
        ),
    };
    let first = AgentChatTransactionRepo::admit_agent_chat_turn(&db, input.clone())
        .await
        .expect("first admission");
    let replay = AgentChatTransactionRepo::admit_agent_chat_turn(&db, input)
        .await
        .expect("replay admission");
    assert_eq!(first.message.id, replay.message.id);
    assert_eq!(first.message.sequence, replay.message.sequence);
    assert_eq!(first.turn.id, replay.turn.id);

    let db_left = db.clone();
    let db_right = db.clone();
    let left = AdmitAgentChatTurn {
        message: user_message("turn-message-2", &chat.id, "2026-08-13T00:00:02.000Z"),
        turn: turn(
            "turn-job-2",
            &chat.id,
            "turn-message-2",
            &identity_id,
            &profile_id,
            "turn-dedupe-2",
            "2026-08-13T00:00:02.000Z",
        ),
    };
    let right = AdmitAgentChatTurn {
        message: user_message("turn-message-3", &chat.id, "2026-08-13T00:00:03.000Z"),
        turn: turn(
            "turn-job-3",
            &chat.id,
            "turn-message-3",
            &identity_id,
            &profile_id,
            "turn-dedupe-3",
            "2026-08-13T00:00:03.000Z",
        ),
    };
    let (left, right) = tokio::join!(
        AgentChatTransactionRepo::admit_agent_chat_turn(&db_left, left),
        AgentChatTransactionRepo::admit_agent_chat_turn(&db_right, right),
    );
    let left = left.expect("left concurrent admission");
    let right = right.expect("right concurrent admission");
    assert_ne!(left.message.sequence, right.message.sequence);
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_message WHERE chat_id = ?")
            .bind(chat.id)
            .fetch_one(db.pool())
            .await
            .expect("message count");
    assert_eq!(count, 3);
}

#[tokio::test]
async fn turn_cancellation_matrix_is_terminal_optimistic_and_idempotent() {
    let (db, account_id, _, identity_id, profile_id) = fixture().await;
    let chat = AgentChatRepo::get_main_chat(&db, &account_id)
        .await
        .expect("chat lookup")
        .expect("chat");

    let admit_db = db.clone();
    let admit_chat_id = chat.id.clone();
    let admit_identity_id = identity_id.clone();
    let admit_profile_id = profile_id.clone();
    let admit = move |job_id: &str, message_id: &str, dedupe: &str| {
        let job_id = job_id.to_owned();
        let message_id = message_id.to_owned();
        let dedupe = dedupe.to_owned();
        let db = admit_db.clone();
        let chat_id = admit_chat_id.clone();
        let identity_id = admit_identity_id.clone();
        let profile_id = admit_profile_id.clone();
        async move {
            AgentChatTransactionRepo::admit_agent_chat_turn(
                &db,
                AdmitAgentChatTurn {
                    message: user_message(&message_id, &chat_id, "2026-08-13T00:00:07.000Z"),
                    turn: turn(
                        &job_id,
                        &chat_id,
                        &message_id,
                        &identity_id,
                        &profile_id,
                        &dedupe,
                        "2026-08-13T00:00:07.000Z",
                    ),
                },
            )
            .await
            .expect("turn admission")
        }
    };

    let queued = admit(
        "cancel-queued",
        "cancel-queued-message",
        "cancel-queued-dedupe",
    )
    .await;
    let queued_cancelled = AgentChatTransactionRepo::cancel_agent_chat_turn(
        &db,
        CancelAgentChatTurn {
            turn_job_id: queued.turn.id.clone(),
            expected_version: queued.turn.version,
            actor_user_id: account_id.clone(),
            idempotency_key: "cancel-queued-key".to_owned(),
            updated_at: "2026-08-13T00:00:08.000Z".to_owned(),
        },
    )
    .await
    .expect("queued turn cancels");
    assert_eq!(queued_cancelled.status, AgentChatTurnState::Cancelled);
    assert_eq!(queued_cancelled.version, queued.turn.version + 1);
    let queued_replay = AgentChatTransactionRepo::cancel_agent_chat_turn(
        &db,
        CancelAgentChatTurn {
            turn_job_id: queued.turn.id.clone(),
            expected_version: queued.turn.version,
            actor_user_id: account_id.clone(),
            idempotency_key: "cancel-queued-key".to_owned(),
            updated_at: "2026-08-13T00:00:09.000Z".to_owned(),
        },
    )
    .await
    .expect("queued cancellation replays");
    assert_eq!(queued_replay.version, queued_cancelled.version);

    let leased = admit(
        "cancel-leased",
        "cancel-leased-message",
        "cancel-leased-dedupe",
    )
    .await;
    let leased = AgentChatTurnJobRepo::update_agent_chat_turn_job(
        &db,
        UpdateAgentChatTurnJob {
            id: leased.turn.id.clone(),
            expected_version: leased.turn.version,
            status: AgentChatTurnState::Leased,
            pending_interaction_id: None,
            lease_owner: Some(Some("cancel-worker".to_owned())),
            leased_until: Some(Some("2026-08-13T00:01:07.000Z".to_owned())),
            attempt_count: Some(1),
            next_attempt_at: Some(None),
            response_message_id: None,
            error_code: None,
            error_message: None,
            updated_at: "2026-08-13T00:00:08.000Z".to_owned(),
        },
    )
    .await
    .expect("leased state persists");
    let leased_cancelled = AgentChatTransactionRepo::cancel_agent_chat_turn(
        &db,
        CancelAgentChatTurn {
            turn_job_id: leased.id.clone(),
            expected_version: leased.version,
            actor_user_id: account_id.clone(),
            idempotency_key: "cancel-leased-key".to_owned(),
            updated_at: "2026-08-13T00:00:09.000Z".to_owned(),
        },
    )
    .await
    .expect("leased turn cancels");
    assert_eq!(leased_cancelled.status, AgentChatTurnState::Cancelled);
    assert_eq!(leased_cancelled.lease_owner, None);

    let retry = admit(
        "cancel-retry",
        "cancel-retry-message",
        "cancel-retry-dedupe",
    )
    .await;
    let retry = AgentChatTurnJobRepo::update_agent_chat_turn_job(
        &db,
        UpdateAgentChatTurnJob {
            id: retry.turn.id.clone(),
            expected_version: retry.turn.version,
            status: AgentChatTurnState::RetryWait,
            pending_interaction_id: None,
            lease_owner: Some(None),
            leased_until: Some(None),
            attempt_count: Some(1),
            next_attempt_at: Some(Some("2026-08-13T00:01:07.000Z".to_owned())),
            response_message_id: None,
            error_code: Some(Some("temporary".to_owned())),
            error_message: Some(Some("retry".to_owned())),
            updated_at: "2026-08-13T00:00:08.000Z".to_owned(),
        },
    )
    .await
    .expect("retry state persists");
    let retry_cancelled = AgentChatTransactionRepo::cancel_agent_chat_turn(
        &db,
        CancelAgentChatTurn {
            turn_job_id: retry.id.clone(),
            expected_version: retry.version,
            actor_user_id: account_id.clone(),
            idempotency_key: "cancel-retry-key".to_owned(),
            updated_at: "2026-08-13T00:00:09.000Z".to_owned(),
        },
    )
    .await
    .expect("retry-wait turn cancels");
    assert_eq!(retry_cancelled.status, AgentChatTurnState::Cancelled);

    let terminal = admit(
        "cancel-terminal",
        "cancel-terminal-message",
        "cancel-terminal-dedupe",
    )
    .await;
    let terminal = AgentChatTurnJobRepo::update_agent_chat_turn_job(
        &db,
        UpdateAgentChatTurnJob {
            id: terminal.turn.id.clone(),
            expected_version: terminal.turn.version,
            status: AgentChatTurnState::Failed,
            pending_interaction_id: None,
            lease_owner: Some(None),
            leased_until: Some(None),
            attempt_count: Some(1),
            next_attempt_at: Some(None),
            response_message_id: None,
            error_code: Some(Some("failed".to_owned())),
            error_message: Some(Some("terminal".to_owned())),
            updated_at: "2026-08-13T00:00:08.000Z".to_owned(),
        },
    )
    .await
    .expect("terminal state persists");
    let terminal_cancel = AgentChatTransactionRepo::cancel_agent_chat_turn(
        &db,
        CancelAgentChatTurn {
            turn_job_id: terminal.id,
            expected_version: terminal.version,
            actor_user_id: account_id,
            idempotency_key: "cancel-terminal-key".to_owned(),
            updated_at: "2026-08-13T00:00:09.000Z".to_owned(),
        },
    )
    .await;
    assert!(matches!(terminal_cancel, Err(DbError::VersionConflict)));

    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event
         WHERE event_type = 'agent_chat.turn.cancelled'",
    )
    .fetch_one(db.pool())
    .await
    .expect("cancellation event count");
    assert_eq!(event_count, 3);
}

#[tokio::test]
async fn response_and_handoff_replays_are_single_ledger_outcomes() {
    let (db, account_id, project_id, identity_id, profile_id) = fixture().await;
    let main = AgentChatRepo::get_main_chat(&db, &account_id)
        .await
        .expect("main chat")
        .expect("main chat exists");
    let project = AgentChatRepo::get_project_chat(&db, &project_id)
        .await
        .expect("project chat")
        .expect("project chat exists");
    let now = "2026-08-13T00:00:04.000Z";
    let admitted = AgentChatTransactionRepo::admit_agent_chat_turn(
        &db,
        AdmitAgentChatTurn {
            message: user_message("complete-trigger", &main.id, now),
            turn: turn(
                "complete-job",
                &main.id,
                "complete-trigger",
                &identity_id,
                &profile_id,
                "complete-dedupe",
                now,
            ),
        },
    )
    .await
    .expect("admit response trigger");
    let leased = AgentChatTurnJobRepo::update_agent_chat_turn_job(
        &db,
        UpdateAgentChatTurnJob {
            id: admitted.turn.id.clone(),
            expected_version: admitted.turn.version,
            status: AgentChatTurnState::Leased,
            pending_interaction_id: None,
            lease_owner: Some(Some("test-owner".to_owned())),
            leased_until: Some(Some("2026-08-13T00:02:04.000Z".to_owned())),
            attempt_count: Some(1),
            // A retry lease may still carry diagnostics from an earlier
            // failed attempt. Successful completion must clear them.
            next_attempt_at: Some(Some("2026-08-13T00:00:05.000Z".to_owned())),
            response_message_id: None,
            error_code: Some(Some("provider_unavailable".to_owned())),
            error_message: Some(Some("first attempt failed".to_owned())),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("lease response trigger");
    let response = CreateAgentChatMessage {
        id: "complete-response".to_owned(),
        chat_id: main.id.clone(),
        sequence: 999,
        author_type: AgentChatMessageAuthorType::Agent,
        author_id: Some(identity_id.clone()),
        content: "complete".to_owned(),
        content_guard_json: "{}".to_owned(),
        sensitivity: "internal".to_owned(),
        status: AgentChatMessageStatus::Complete,
        outcome: Some("ok".to_owned()),
        model: Some("test-model".to_owned()),
        profile_id: Some(profile_id.clone()),
        session_id: None,
        context_manifest_id: None,
        token_usage_json: None,
        duration_ms: Some(3),
        error: None,
        correlation_id: "corr-response".to_owned(),
        causation_id: Some(admitted.message.id.clone()),
        handoff_id: None,
        source_type: "native".to_owned(),
        source_id: None,
        source_message_id: None,
        source_room_id: None,
        source_conversation_id: None,
        source_sequence: None,
        source_metadata_json: "{}".to_owned(),
        created_at: now.to_owned(),
    };
    let wrong_owner = AgentChatTransactionRepo::complete_agent_chat_turn(
        &db,
        db::CompleteAgentChatTurn {
            turn_job_id: admitted.turn.id.clone(),
            expected_version: leased.version,
            lease_owner: "wrong-worker".to_owned(),
            response: response.clone(),
            updated_at: now.to_owned(),
        },
    )
    .await;
    assert!(matches!(wrong_owner, Err(db::DbError::VersionConflict)));
    let completed = AgentChatTransactionRepo::complete_agent_chat_turn(
        &db,
        db::CompleteAgentChatTurn {
            turn_job_id: admitted.turn.id.clone(),
            expected_version: leased.version,
            lease_owner: "test-owner".to_owned(),
            response: response.clone(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("complete response");
    assert!(completed.turn.next_attempt_at.is_none());
    assert!(completed.turn.error_code.is_none());
    assert!(completed.turn.error_message.is_none());
    let replay = AgentChatTransactionRepo::complete_agent_chat_turn(
        &db,
        db::CompleteAgentChatTurn {
            turn_job_id: admitted.turn.id,
            expected_version: 1,
            lease_owner: "test-owner".to_owned(),
            response,
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("replay response");
    assert_eq!(completed.response.id, replay.response.id);
    assert_eq!(replay.turn.status.to_string(), "succeeded");
    let response_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_chat_message WHERE id = 'complete-response'",
    )
    .fetch_one(db.pool())
    .await
    .expect("response count");
    assert_eq!(response_count, 1);

    let handoff = CreateAgentHandoff {
        id: "handoff-1".to_owned(),
        source_chat_id: main.id.clone(),
        target_chat_id: project.id.clone(),
        source_message_id: Some(completed.response.id.clone()),
        source_turn_job_id: None,
        author_identity_id: Some(identity_id.clone()),
        content: "bounded handoff".to_owned(),
        content_guard_json: "{}".to_owned(),
        source_revisions_json: "[]".to_owned(),
        correlation_id: "corr-handoff".to_owned(),
        causation_id: Some(completed.response.id.clone()),
        dedupe_key: "handoff-dedupe".to_owned(),
        created_at: now.to_owned(),
        updated_at: now.to_owned(),
    };
    let target_message = CreateAgentChatMessage {
        id: "handoff-message".to_owned(),
        chat_id: project.id.clone(),
        sequence: 999,
        author_type: AgentChatMessageAuthorType::Handoff,
        author_id: Some(identity_id.clone()),
        content: handoff.content.clone(),
        content_guard_json: "{}".to_owned(),
        sensitivity: "internal".to_owned(),
        status: AgentChatMessageStatus::Complete,
        outcome: None,
        model: None,
        profile_id: Some(profile_id.clone()),
        session_id: None,
        context_manifest_id: None,
        token_usage_json: None,
        duration_ms: None,
        error: None,
        correlation_id: "corr-handoff-message".to_owned(),
        causation_id: Some(completed.response.id),
        handoff_id: None,
        source_type: "handoff".to_owned(),
        source_id: Some(handoff.id.clone()),
        source_message_id: None,
        source_room_id: None,
        source_conversation_id: None,
        source_sequence: None,
        source_metadata_json: "{}".to_owned(),
        created_at: now.to_owned(),
    };
    let target_turn = turn(
        "handoff-job",
        &project.id,
        "handoff-message",
        &identity_id,
        &profile_id,
        "handoff-turn-dedupe",
        now,
    );
    let admitted_handoff = AgentChatTransactionRepo::admit_agent_handoff(
        &db,
        AdmitAgentHandoff {
            handoff: handoff.clone(),
            target_message: target_message.clone(),
            target_turn: target_turn.clone(),
            source_responder_provenance_json: None,
        },
    )
    .await
    .expect("handoff admission");
    let replay_handoff = AgentChatTransactionRepo::admit_agent_handoff(
        &db,
        AdmitAgentHandoff {
            handoff,
            target_message,
            target_turn,
            source_responder_provenance_json: None,
        },
    )
    .await
    .expect("handoff replay");
    assert_eq!(admitted_handoff.handoff.id, replay_handoff.handoff.id);
    assert_eq!(admitted_handoff.message.id, replay_handoff.message.id);
    assert_eq!(admitted_handoff.turn.id, replay_handoff.turn.id);
    let delivery_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_handoff_delivery WHERE handoff_id = 'handoff-1'",
    )
    .fetch_one(db.pool())
    .await
    .expect("delivery count");
    assert_eq!(delivery_count, 1);
}

#[tokio::test]
async fn failed_turn_emits_one_bounded_replay_safe_event() {
    let (db, account_id, _, identity_id, profile_id) = fixture().await;
    let chat = AgentChatRepo::get_main_chat(&db, &account_id)
        .await
        .expect("chat lookup")
        .expect("chat");
    let now = "2026-08-13T00:00:06.000Z";
    let admitted = AgentChatTransactionRepo::admit_agent_chat_turn(
        &db,
        AdmitAgentChatTurn {
            message: user_message("failed-trigger", &chat.id, now),
            turn: turn(
                "failed-job",
                &chat.id,
                "failed-trigger",
                &identity_id,
                &profile_id,
                "failed-dedupe",
                now,
            ),
        },
    )
    .await
    .expect("admit failed trigger");
    let leased = AgentChatTurnJobRepo::update_agent_chat_turn_job(
        &db,
        UpdateAgentChatTurnJob {
            id: admitted.turn.id.clone(),
            expected_version: admitted.turn.version,
            status: AgentChatTurnState::Leased,
            pending_interaction_id: None,
            lease_owner: Some(Some("failure-owner".to_owned())),
            leased_until: Some(Some("2026-08-13T00:07:06.000Z".to_owned())),
            attempt_count: Some(1),
            next_attempt_at: Some(None),
            response_message_id: None,
            error_code: None,
            error_message: None,
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("lease failed trigger");

    let long_error = "adapter output ".repeat(200);
    let failed = AgentChatTransactionRepo::fail_agent_chat_turn(
        &db,
        FailAgentChatTurn {
            turn_job_id: leased.id.clone(),
            expected_version: leased.version,
            lease_owner: "failure-owner".to_owned(),
            status: AgentChatTurnState::Failed,
            attempt_count: leased.attempt_count,
            next_attempt_at: None,
            error_code: "adapter_failed".to_owned(),
            error_message: long_error.clone(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("failure persists");
    assert_eq!(failed.status, AgentChatTurnState::Failed);

    let event = DomainEventRepo::get_event_by_dedupe(
        &db,
        &format!(
            "agent-chat-event:agent_chat.turn.failed:{}:{}",
            leased.id, leased.version
        ),
    )
    .await
    .expect("failure event lookup")
    .expect("failure event exists");
    assert_eq!(event.event_type, "agent_chat.turn.failed");
    assert_eq!(event.entity_type, "agent_chat_turn_job");
    assert_eq!(event.scope_type, "agent_chat");
    let payload: serde_json::Value =
        serde_json::from_str(&event.payload_json).expect("failure event payload");
    assert_eq!(payload["status"], "failed");
    assert_eq!(payload["responder_identity_id"], identity_id);
    assert_eq!(
        payload["error_message"].as_str().expect("error text").len(),
        512
    );
    assert!(!event.payload_json.contains(&long_error));

    let replay = AgentChatTransactionRepo::fail_agent_chat_turn(
        &db,
        FailAgentChatTurn {
            turn_job_id: leased.id.clone(),
            expected_version: leased.version,
            lease_owner: "failure-owner".to_owned(),
            status: AgentChatTurnState::Failed,
            attempt_count: leased.attempt_count,
            next_attempt_at: None,
            error_code: "adapter_failed".to_owned(),
            error_message: long_error,
            updated_at: now.to_owned(),
        },
    )
    .await;
    assert!(matches!(replay, Err(DbError::VersionConflict)));
    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event
         WHERE event_type = 'agent_chat.turn.failed' AND entity_id = ?",
    )
    .bind(&leased.id)
    .fetch_one(db.pool())
    .await
    .expect("failure event count");
    assert_eq!(event_count, 1);
}

#[tokio::test]
async fn failed_composite_rolls_back_message_and_turn() {
    let (db, account_id, _, identity_id, profile_id) = fixture().await;
    let chat = AgentChatRepo::get_main_chat(&db, &account_id)
        .await
        .expect("chat lookup")
        .expect("chat");
    let result = AgentChatTransactionRepo::admit_agent_chat_turn(
        &db,
        AdmitAgentChatTurn {
            message: {
                let mut message =
                    user_message("rollback-message", &chat.id, "2026-08-13T00:00:05.000Z");
                message.source_type = "invalid".to_owned();
                message
            },
            turn: turn(
                "rollback-job",
                &chat.id,
                "rollback-message",
                &identity_id,
                &profile_id,
                "rollback-dedupe",
                "2026-08-13T00:00:05.000Z",
            ),
        },
    )
    .await;
    assert!(result.is_err());
    let message_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_message WHERE id = 'rollback-message'")
            .fetch_one(db.pool())
            .await
            .expect("rollback message count");
    let job_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_turn_job WHERE id = 'rollback-job'")
            .fetch_one(db.pool())
            .await
            .expect("rollback job count");
    assert_eq!(message_count, 0);
    assert_eq!(job_count, 0);
}

#[tokio::test]
async fn failure_persistence_survives_sqlite_contention_and_restart() {
    let path = unique_sqlite_path();
    let url = sqlite_url(&path);
    let db = database_url(&url).await;
    let (db, account_id, _project_id, identity_id, profile_id) = fixture_on(db).await;
    let chat = AgentChatRepo::get_main_chat(&db, &account_id)
        .await
        .expect("chat lookup")
        .expect("main chat exists");
    let now = "2026-08-13T00:00:10.000Z";
    let admitted = AgentChatTransactionRepo::admit_agent_chat_turn(
        &db,
        AdmitAgentChatTurn {
            message: user_message("failure-trigger", &chat.id, now),
            turn: turn(
                "failure-job",
                &chat.id,
                "failure-trigger",
                &identity_id,
                &profile_id,
                "failure-dedupe",
                now,
            ),
        },
    )
    .await
    .expect("failure trigger admits");
    let leased = AgentChatTurnJobRepo::update_agent_chat_turn_job(
        &db,
        UpdateAgentChatTurnJob {
            id: admitted.turn.id.clone(),
            expected_version: admitted.turn.version,
            status: AgentChatTurnState::Leased,
            pending_interaction_id: None,
            lease_owner: Some(Some("failure-worker-a".to_owned())),
            leased_until: Some(Some("2026-08-13T00:01:10.000Z".to_owned())),
            attempt_count: Some(1),
            next_attempt_at: Some(None),
            response_message_id: Some(None),
            error_code: Some(None),
            error_message: Some(None),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("failure trigger leases");

    // A second pool represents another worker/process.  Both workers persist
    // the same failure using the same lease version; SQLite serialization plus
    // the optimistic predicate must produce exactly one durable winner.
    let contender = database_url(&url).await;
    let left = FailAgentChatTurn {
        turn_job_id: leased.id.clone(),
        expected_version: leased.version,
        lease_owner: "failure-worker-a".to_owned(),
        status: AgentChatTurnState::RetryWait,
        attempt_count: 1,
        next_attempt_at: Some("2026-08-13T00:00:15.000Z".to_owned()),
        error_code: "provider_unavailable".to_owned(),
        error_message: "provider unavailable".to_owned(),
        updated_at: "2026-08-13T00:00:11.000Z".to_owned(),
    };
    let right = FailAgentChatTurn {
        turn_job_id: leased.id.clone(),
        expected_version: leased.version,
        lease_owner: "failure-worker-a".to_owned(),
        status: AgentChatTurnState::RetryWait,
        attempt_count: 1,
        next_attempt_at: Some("2026-08-13T00:00:15.000Z".to_owned()),
        error_code: "provider_unavailable".to_owned(),
        error_message: "provider unavailable".to_owned(),
        updated_at: "2026-08-13T00:00:11.000Z".to_owned(),
    };
    let (left, right) = tokio::join!(
        AgentChatTransactionRepo::fail_agent_chat_turn(&db, left),
        AgentChatTransactionRepo::fail_agent_chat_turn(&contender, right),
    );
    let winner = match (left, right) {
        (Ok(winner), Err(DbError::VersionConflict))
        | (Err(DbError::VersionConflict), Ok(winner)) => winner,
        (left, right) => panic!("failure contention did not serialize: {left:?}; {right:?}"),
    };
    assert_eq!(winner.status, AgentChatTurnState::RetryWait);
    assert_eq!(winner.attempt_count, 1);
    assert_eq!(winner.max_attempts, 3);
    assert!(winner.lease_owner.is_none(), "failure must clear the lease");
    assert!(
        winner.leased_until.is_none(),
        "failure must clear lease expiry"
    );

    let persisted = AgentChatTurnJobRepo::get_agent_chat_turn_job(&db, &winner.id)
        .await
        .expect("persisted failure reads")
        .expect("persisted failure exists");
    assert_eq!(persisted.status, AgentChatTurnState::RetryWait);
    assert_eq!(persisted.attempt_count, 1);
    assert_eq!(persisted.max_attempts, 3);
    assert_eq!(
        persisted.error_code.as_deref(),
        Some("provider_unavailable")
    );
    assert_eq!(
        persisted.error_message.as_deref(),
        Some("provider unavailable")
    );
    assert!(persisted.lease_owner.is_none());
    assert!(persisted.leased_until.is_none());
    assert!(persisted.response_message_id.is_none());
    let response_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_chat_message
         WHERE chat_id = ? AND author_type = 'agent'",
    )
    .bind(&chat.id)
    .fetch_one(db.pool())
    .await
    .expect("response count reads");
    assert_eq!(response_count, 0, "failure must not create a response");

    // Reopen the same file as a fresh worker.  The retry state and consumed
    // attempt must come from SQLite, not from an in-memory worker counter.
    drop(contender);
    drop(db);
    let restarted = database_url(&url).await;
    let recovered = AgentChatTurnJobRepo::get_agent_chat_turn_job(&restarted, &winner.id)
        .await
        .expect("restart reads turn")
        .expect("turn survives restart");
    assert_eq!(recovered.status, AgentChatTurnState::RetryWait);
    assert_eq!(recovered.attempt_count, 1);
    assert_eq!(recovered.max_attempts, 3);
    assert!(recovered.lease_owner.is_none());
    assert!(recovered.leased_until.is_none());
    assert!(recovered.response_message_id.is_none());

    let leased_after_restart = AgentChatTurnJobRepo::update_agent_chat_turn_job(
        &restarted,
        UpdateAgentChatTurnJob {
            id: recovered.id.clone(),
            expected_version: recovered.version,
            status: AgentChatTurnState::Leased,
            pending_interaction_id: None,
            lease_owner: Some(Some("failure-worker-after-restart".to_owned())),
            leased_until: Some(Some("2026-08-13T00:02:10.000Z".to_owned())),
            attempt_count: Some(2),
            next_attempt_at: Some(None),
            response_message_id: Some(None),
            error_code: None,
            error_message: None,
            updated_at: "2026-08-13T00:01:00.000Z".to_owned(),
        },
    )
    .await
    .expect("restarted worker leases the next bounded attempt");
    let second_failure = AgentChatTransactionRepo::fail_agent_chat_turn(
        &restarted,
        FailAgentChatTurn {
            turn_job_id: leased_after_restart.id.clone(),
            expected_version: leased_after_restart.version,
            lease_owner: "failure-worker-after-restart".to_owned(),
            status: AgentChatTurnState::RetryWait,
            attempt_count: 2,
            next_attempt_at: Some("2026-08-13T00:01:10.000Z".to_owned()),
            error_code: "provider_unavailable".to_owned(),
            error_message: "provider unavailable again".to_owned(),
            updated_at: "2026-08-13T00:01:01.000Z".to_owned(),
        },
    )
    .await
    .expect("restarted failure persists");
    assert_eq!(second_failure.attempt_count, 2);
    assert_eq!(second_failure.max_attempts, 3);
    assert!(second_failure.lease_owner.is_none());
    assert!(second_failure.leased_until.is_none());

    drop(restarted);
    let final_db = database_url(&url).await;
    let final_state = AgentChatTurnJobRepo::get_agent_chat_turn_job(&final_db, &winner.id)
        .await
        .expect("final restart reads turn")
        .expect("turn remains durable");
    assert_eq!(final_state.status, AgentChatTurnState::RetryWait);
    assert_eq!(final_state.attempt_count, 2);
    assert_eq!(final_state.max_attempts, 3);
    assert!(final_state.lease_owner.is_none());
    assert!(final_state.leased_until.is_none());
    assert!(final_state.response_message_id.is_none());
    drop(final_db);

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
}
