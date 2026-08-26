//! V103 Main Chat topic boundary (design D21, live-acceptance finding F18).
//!
//! Covers:
//! - the backfill creates exactly one topic per existing Main Chat and
//!   changes no existing `agent_chat_message`/`agent_chat_turn_job` id or
//!   provenance (test (a) named in the task method: before this migration a
//!   fresh topic boundary does not exist at all);
//! - a rotation appends a real, visible divider message and is idempotent on
//!   a replayed topic id;
//! - a rotation is denied while a Main turn is live, or while a Product
//!   Genesis session still needs an explicit finish-or-cancel decision
//!   (test (c) named in the task method).

use db::{
    create_sqlite_pool, now_rfc3339, run_migrations_from, topic_divider_message,
    AgentChatMessageAuthorType, AgentChatMessageRepo, AgentChatMessageStatus, AgentChatRepo,
    AgentChatTopicDenialReason, AgentChatTopicRepo, AgentChatTopicTransactionRepo,
    AgentChatTurnJobRepo, AgentRepo, AgentStatus, CreateAgentChatMessage, CreateAgentChatTopic,
    CreateAgentChatTurnJob, CreateAgentIdentity, CreateAgentProfile, RotateAgentChatTopic,
    SqliteDb, User, UserRepo,
};
use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

fn unique_temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "forge-main-chat-topics-{name}-{}-{nanos}",
        std::process::id()
    ))
}

fn migration_version(filename: &str) -> Option<i64> {
    filename.strip_prefix('V')?.split_once("__")?.0.parse().ok()
}

/// Copy every migration file whose version is in `(min, max]` into
/// `destination`, so a test can apply the schema up to just before V103 and
/// then apply V103 alone as its own step.
fn copy_migrations_in_range(min_exclusive: i64, max_inclusive: i64, destination: &Path) {
    let source_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    for entry in fs::read_dir(source_dir).expect("migration dir reads") {
        let entry = entry.expect("migration entry reads");
        let path = entry.path();
        let Some(filename) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(version) = migration_version(filename) else {
            continue;
        };
        if version > min_exclusive && version <= max_inclusive {
            fs::copy(&path, destination.join(filename)).expect("migration copies");
        }
    }
}

/// Apply every migration through V102 (everything before the Main Chat topic
/// boundary), returning the pool so the caller can insert pre-V103 fixture
/// data before applying V103 as a separate, observable step.
async fn database_before_v103(name: &str) -> (SqliteDb, PathBuf) {
    let migration_dir = unique_temp_dir(name);
    fs::create_dir_all(&migration_dir).expect("migration dir creates");
    copy_migrations_in_range(0, 102, &migration_dir);
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("pre-V103 migrations apply");
    (SqliteDb::new(pool), migration_dir)
}

async fn apply_v103(db: &SqliteDb, migration_dir: &Path) {
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("migrations")
        .join("V103__main_chat_topics.sql");
    fs::copy(&source, migration_dir.join("V103__main_chat_topics.sql"))
        .expect("V103 migration copies");
    run_migrations_from(db.pool(), migration_dir)
        .await
        .expect("V103 migration applies");
}

async fn create_user(db: &SqliteDb, id: &str, now: &str) {
    UserRepo::create_user(
        db,
        &User {
            id: id.to_owned(),
            email: format!("{id}@example.test"),
            password_hash: "test".to_owned(),
            display_name: Some(id.to_owned()),
            is_admin: false,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("user creates");
}

fn message_fixture(id: &str, chat_id: &str, content: &str, now: &str) -> CreateAgentChatMessage {
    CreateAgentChatMessage {
        id: id.to_owned(),
        chat_id: chat_id.to_owned(),
        sequence: 0,
        author_type: AgentChatMessageAuthorType::User,
        author_id: Some("historical-user".to_owned()),
        content: content.to_owned(),
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
        correlation_id: format!("correlation-{id}"),
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

/// V103's backfill must create exactly one topic per existing Main Chat and
/// must not change a single pre-existing message's id, sequence, content, or
/// any other provenance field.
#[tokio::test]
async fn backfill_creates_one_topic_and_preserves_every_historical_message() {
    let (db, migration_dir) = database_before_v103("backfill").await;
    let now = now_rfc3339();
    create_user(&db, "topic-backfill-user", &now).await;
    let chat = AgentChatRepo::get_main_chat(&db, "topic-backfill-user")
        .await
        .expect("main chat lookup")
        .expect("V071 trigger creates the Main Chat on user insert");

    let first = message_fixture("hist-msg-1", &chat.id, "first historical message", &now);
    let second = message_fixture("hist-msg-2", &chat.id, "second historical message", &now);
    let first_before = AgentChatMessageRepo::append_agent_chat_message(&db, first)
        .await
        .expect("first historical message appends");
    let second_before = AgentChatMessageRepo::append_agent_chat_message(&db, second)
        .await
        .expect("second historical message appends");
    assert_eq!(first_before.sequence, 0);
    assert_eq!(second_before.sequence, 1);

    apply_v103(&db, &migration_dir).await;

    // Exactly one topic, backfilled at sequence 0 and covering every
    // historical message from the start.
    let topics = AgentChatTopicRepo::list_agent_chat_topics(&db, &chat.id)
        .await
        .expect("topics list");
    assert_eq!(topics.len(), 1, "exactly one backfilled topic");
    let topic = &topics[0];
    assert_eq!(topic.chat_id, chat.id);
    assert_eq!(topic.sequence, 0);
    assert_eq!(topic.starting_message_sequence, 0);
    assert_eq!(topic.starting_message_id, None);
    assert_eq!(topic.principal_type, "system");
    assert_eq!(topic.principal_id, None);
    assert_eq!(topic.label, "Original conversation");

    let current = AgentChatTopicRepo::get_current_agent_chat_topic(&db, &chat.id)
        .await
        .expect("current topic lookup")
        .expect("a current topic exists");
    assert_eq!(current.id, topic.id);

    // Every historical message id/sequence/content/provenance is byte-for-
    // byte unchanged -- V103 issues no UPDATE against `agent_chat_message`.
    let first_after = AgentChatMessageRepo::get_agent_chat_message(&db, "hist-msg-1")
        .await
        .expect("first message re-reads")
        .expect("first message still exists under its original id");
    let second_after = AgentChatMessageRepo::get_agent_chat_message(&db, "hist-msg-2")
        .await
        .expect("second message re-reads")
        .expect("second message still exists under its original id");
    assert_eq!(first_after, first_before);
    assert_eq!(second_after, second_before);
}

/// The Main Chat is ready *before* V103 applies, so the backfill gives it
/// topic 0 -- exercising the realistic upgrade path where a test's own
/// rotation becomes topic 1, not topic 0.
async fn database_with_ready_main_chat(name: &str) -> (SqliteDb, String) {
    let (db, migration_dir) = database_before_v103(name).await;
    let now = now_rfc3339();
    let account_id = format!("{name}-account");
    create_user(&db, &account_id, &now).await;
    apply_v103(&db, &migration_dir).await;
    let chat = AgentChatRepo::get_main_chat(&db, &account_id)
        .await
        .expect("main chat lookup")
        .expect("V071 trigger creates the Main Chat on user insert");
    (db, chat.id)
}

fn rotate_input(chat_id: &str, topic_id: &str, label: &str) -> RotateAgentChatTopic {
    let now = now_rfc3339();
    RotateAgentChatTopic {
        topic: CreateAgentChatTopic {
            id: topic_id.to_owned(),
            chat_id: chat_id.to_owned(),
            label: label.to_owned(),
            summary: None,
            principal_type: "user".to_owned(),
            principal_id: Some("rotating-user".to_owned()),
            created_at: now.clone(),
        },
        divider_message: topic_divider_message(
            format!("divider-{topic_id}"),
            chat_id.to_owned(),
            label,
            format!("correlation-{topic_id}"),
            now,
        ),
    }
}

#[tokio::test]
async fn rotate_appends_a_visible_divider_and_is_idempotent_on_replay() {
    let (db, chat_id) = database_with_ready_main_chat("rotate-happy").await;

    let outcome = AgentChatTopicTransactionRepo::rotate_agent_chat_topic(
        &db,
        rotate_input(&chat_id, "topic-2", "Second topic"),
    )
    .await
    .expect("rotate succeeds")
    .expect("rotate is not denied");
    assert_eq!(outcome.topic.sequence, 1, "second topic is sequence 1");
    assert_eq!(
        outcome.topic.starting_message_id.as_deref(),
        Some(outcome.divider_message.id.as_str())
    );
    assert_eq!(
        outcome.topic.starting_message_sequence,
        outcome.divider_message.sequence
    );
    assert_eq!(outcome.divider_message.author_type.to_string(), "system");
    assert_eq!(
        outcome.divider_message.content,
        "New topic started: Second topic"
    );

    let current = AgentChatTopicRepo::get_current_agent_chat_topic(&db, &chat_id)
        .await
        .expect("current topic lookup")
        .expect("a current topic exists");
    assert_eq!(current.id, "topic-2");

    // Replaying the exact same topic id returns the already-committed
    // rotation rather than rotating a second time.
    let replay = AgentChatTopicTransactionRepo::rotate_agent_chat_topic(
        &db,
        rotate_input(&chat_id, "topic-2", "Second topic"),
    )
    .await
    .expect("replay succeeds")
    .expect("replay is not denied");
    assert_eq!(replay, outcome);
    let topics = AgentChatTopicRepo::list_agent_chat_topics(&db, &chat_id)
        .await
        .expect("topics list");
    assert_eq!(topics.len(), 2, "replay does not create a duplicate topic");
}

#[tokio::test]
async fn rotate_is_denied_while_a_main_turn_is_live() {
    let (db, chat_id) = database_with_ready_main_chat("rotate-live-turn").await;
    let now = now_rfc3339();
    let identity_id = "live-turn-identity".to_owned();
    let profile_id = "live-turn-profile".to_owned();
    AgentRepo::create_identity_with_profile(
        &db,
        CreateAgentIdentity {
            id: identity_id.clone(),
            name: "Live Turn Responder".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
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
    .expect("responder identity/profile creates");
    let trigger = AgentChatMessageRepo::append_agent_chat_message(
        &db,
        message_fixture("live-turn-trigger", &chat_id, "start a turn", &now),
    )
    .await
    .expect("trigger message appends");

    AgentChatTurnJobRepo::create_agent_chat_turn_job(
        &db,
        CreateAgentChatTurnJob {
            id: "live-turn".to_owned(),
            chat_id: chat_id.clone(),
            triggering_message_id: trigger.id.clone(),
            responder_identity_id: identity_id,
            profile_id,
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
            canonical_scope_id: chat_id.clone(),
            dedupe_key: "live-turn-dedupe".to_owned(),
            max_attempts: 3,
            correlation_id: "live-turn-correlation".to_owned(),
            causation_id: None,
            causation_depth: 0,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("live turn job creates (queued by default)");

    let denial = AgentChatTopicTransactionRepo::rotate_agent_chat_topic(
        &db,
        rotate_input(&chat_id, "topic-during-live-turn", "Blocked topic"),
    )
    .await
    .expect("rotate call succeeds at the transport level")
    .expect_err("rotate is denied while a Main turn is live");
    assert_eq!(denial, AgentChatTopicDenialReason::MainTurnLive);

    // Denial does not create a topic or a divider message.
    let topics = AgentChatTopicRepo::list_agent_chat_topics(&db, &chat_id)
        .await
        .expect("topics list");
    assert_eq!(topics.len(), 1, "only the backfilled topic exists");
    let orphan_divider =
        AgentChatMessageRepo::get_agent_chat_message(&db, "divider-topic-during-live-turn")
            .await
            .expect("divider lookup");
    assert!(orphan_divider.is_none());
}

#[tokio::test]
async fn rotate_is_denied_while_a_genesis_session_needs_a_decision() {
    let (db, chat_id) = database_with_ready_main_chat("rotate-genesis").await;
    let account_id = "rotate-genesis-account";
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO product_genesis_session (
            id, account_id, main_chat_id, prompt_revision, prompt_body, maturity,
            lifecycle, version, created_at, updated_at
         ) VALUES (?, ?, ?, 'v1', 'prompt body', 'mvp', 'discovering', 1, ?, ?)",
    )
    .bind("genesis-pending")
    .bind(account_id)
    .bind(&chat_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("pending genesis session inserts");

    let denial = AgentChatTopicTransactionRepo::rotate_agent_chat_topic(
        &db,
        rotate_input(&chat_id, "topic-during-genesis", "Blocked topic"),
    )
    .await
    .expect("rotate call succeeds at the transport level")
    .expect_err("rotate is denied while Genesis needs a finish-or-cancel decision");
    assert_eq!(denial, AgentChatTopicDenialReason::GenesisDecisionPending);

    let topics = AgentChatTopicRepo::list_agent_chat_topics(&db, &chat_id)
        .await
        .expect("topics list");
    assert_eq!(topics.len(), 1, "only the backfilled topic exists");
}
