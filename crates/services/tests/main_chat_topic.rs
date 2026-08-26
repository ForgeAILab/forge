//! `MainChatTopicService` acceptance coverage (design D21, live-acceptance
//! finding F18: "the singular Main Chat has no fresh-topic boundary").
//!
//! These tests exercise the service boundary (authorization, denial
//! translation, label defaulting) on top of the DB-layer contract already
//! proven directly in `crates/db/tests/main_chat_topics.rs`.

use std::sync::Arc;

use db::{
    create_sqlite_pool, now_rfc3339, run_migrations, AgentChatRepo, AgentChatTurnJobRepo,
    AgentRepo, AgentStatus, CreateAgentIdentity, CreateAgentProfile, SqliteDb, User, UserRepo,
};
use services::{
    AgentChatService, MainChatTopicService, ProductGenesisService, SendAgentChatMessageInput,
    ServiceError, SetMainAgentBindingInput, StartMainChatTopicInput,
};

const ACCOUNT_ID: &str = "topic-service-account";
const OTHER_ACCOUNT_ID: &str = "topic-service-other-account";
const IDENTITY_ID: &str = "topic-service-identity";
const PROFILE_ID: &str = "topic-service-profile";

async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    Arc::new(SqliteDb::new(pool))
}

struct Fixture {
    db: Arc<SqliteDb>,
    chat_service: Arc<AgentChatService<SqliteDb>>,
    topics: MainChatTopicService<SqliteDb>,
    chat_id: String,
}

async fn fixture() -> Fixture {
    let db = database().await;
    let now = now_rfc3339();
    UserRepo::create_user(
        &*db,
        &User {
            id: ACCOUNT_ID.to_owned(),
            email: "topic-service@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Topic Service Owner".to_owned()),
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("account creates");
    UserRepo::create_user(
        &*db,
        &User {
            id: OTHER_ACCOUNT_ID.to_owned(),
            email: "topic-service-other@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Other Owner".to_owned()),
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("other account creates");

    AgentRepo::create_identity_with_profile(
        &*db,
        CreateAgentIdentity {
            id: IDENTITY_ID.to_owned(),
            name: "Main Agent".to_owned(),
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
            id: PROFILE_ID.to_owned(),
            identity_id: IDENTITY_ID.to_owned(),
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

    let chat_service = Arc::new(AgentChatService::new(Arc::clone(&db)));
    chat_service
        .set_main_binding(SetMainAgentBindingInput {
            actor_user_id: ACCOUNT_ID.to_owned(),
            account_id: ACCOUNT_ID.to_owned(),
            identity_id: IDENTITY_ID.to_owned(),
            autonomy_policy_json: "{}".to_owned(),
            tool_policy_revision: "default".to_owned(),
            expected_version: None,
            replacement_reason: None,
        })
        .await
        .expect("main binding activates the chat");
    let chat = AgentChatRepo::get_main_chat(&*db, ACCOUNT_ID)
        .await
        .expect("main chat lookup")
        .expect("main chat exists");

    let topics = MainChatTopicService::new(
        Arc::clone(&db),
        Arc::clone(&chat_service),
        ProductGenesisService::for_sqlite(Arc::clone(&db)),
    );

    Fixture {
        db,
        chat_service,
        topics,
        chat_id: chat.id,
    }
}

/// This fixture's account is created *after* `run_migrations` (which
/// includes V103) already ran, so it never goes through the V103 backfill --
/// that backfill only covers Main Chats that already existed at migration
/// time (`crates/db/tests/main_chat_topics.rs` covers that path directly).
/// A brand-new Main Chat legitimately starts with zero topics; a floor of
/// "no topic yet" behaves exactly like one topic spanning the whole history
/// (the turn worker's floor defaults to sequence 0), so this is not a gap --
/// the first `start_topic` call simply becomes this chat's topic 0.
#[tokio::test]
async fn starting_a_topic_rotates_a_fresh_main_chat_past_its_first_topic() {
    let fixture = fixture().await;

    let before = fixture
        .topics
        .list_topics(ACCOUNT_ID, &fixture.chat_id)
        .await
        .expect("initial topic list");
    assert!(before.is_empty(), "a fresh Main Chat starts with no topics");
    assert!(fixture
        .topics
        .current_topic(ACCOUNT_ID, &fixture.chat_id)
        .await
        .expect("current topic lookup")
        .is_none());

    let first = fixture
        .topics
        .start_topic(StartMainChatTopicInput {
            actor_user_id: ACCOUNT_ID.to_owned(),
            chat_id: fixture.chat_id.clone(),
            label: Some("Onboarding".to_owned()),
            summary: None,
        })
        .await
        .expect("first topic starts");
    assert_eq!(first.topic.sequence, 0);

    let second = fixture
        .topics
        .start_topic(StartMainChatTopicInput {
            actor_user_id: ACCOUNT_ID.to_owned(),
            chat_id: fixture.chat_id.clone(),
            label: Some("Planning the next release".to_owned()),
            summary: Some("Switching context away from onboarding chatter.".to_owned()),
        })
        .await
        .expect("second topic rotates");
    assert_eq!(second.topic.sequence, 1);
    assert_eq!(second.topic.label, "Planning the next release");
    assert_eq!(second.divider_message.chat_id, fixture.chat_id);
    assert_eq!(
        second.divider_message.content,
        "New topic started: Planning the next release"
    );

    let after = fixture
        .topics
        .list_topics(ACCOUNT_ID, &fixture.chat_id)
        .await
        .expect("topic list after two rotations");
    assert_eq!(after.len(), 2);
    assert_eq!(after[0].id, first.topic.id);
    assert_eq!(after[1].id, second.topic.id);
    let current_after = fixture
        .topics
        .current_topic(ACCOUNT_ID, &fixture.chat_id)
        .await
        .expect("current topic lookup")
        .expect("a current topic exists");
    assert_eq!(
        current_after.id, second.topic.id,
        "the newest topic is current"
    );
}

#[tokio::test]
async fn starting_a_topic_with_no_label_gets_a_server_default() {
    let fixture = fixture().await;
    let rotation = fixture
        .topics
        .start_topic(StartMainChatTopicInput {
            actor_user_id: ACCOUNT_ID.to_owned(),
            chat_id: fixture.chat_id.clone(),
            label: None,
            summary: None,
        })
        .await
        .expect("topic rotates with a default label");
    assert_eq!(rotation.topic.label, "New topic");
}

#[tokio::test]
async fn starting_a_topic_denies_a_caller_who_does_not_own_the_chat() {
    let fixture = fixture().await;
    let error = fixture
        .topics
        .start_topic(StartMainChatTopicInput {
            actor_user_id: OTHER_ACCOUNT_ID.to_owned(),
            chat_id: fixture.chat_id.clone(),
            label: Some("Not mine".to_owned()),
            summary: None,
        })
        .await
        .expect_err("a different account cannot rotate this Main Chat's topic");
    assert!(matches!(error, ServiceError::NotFound { .. }));
}

#[tokio::test]
async fn starting_a_topic_is_denied_while_a_main_turn_is_live() {
    let fixture = fixture().await;
    fixture
        .chat_service
        .send_message(SendAgentChatMessageInput {
            actor_user_id: ACCOUNT_ID.to_owned(),
            chat_id: fixture.chat_id.clone(),
            content: "Kick off a turn".to_owned(),
            dedupe_key: None,
        })
        .await
        .expect("sending a message admits a live (queued) turn");

    let error = fixture
        .topics
        .start_topic(StartMainChatTopicInput {
            actor_user_id: ACCOUNT_ID.to_owned(),
            chat_id: fixture.chat_id.clone(),
            label: Some("Should be blocked".to_owned()),
            summary: None,
        })
        .await
        .expect_err("a live Main turn denies the topic reset");
    assert!(matches!(error, ServiceError::Conflict(message) if message.contains("Main turn")));

    let turns = AgentChatTurnJobRepo::list_agent_chat_turn_jobs(&*fixture.db, &fixture.chat_id)
        .await
        .expect("turn list");
    assert_eq!(turns.len(), 1, "the live turn is untouched by the denial");
}

#[tokio::test]
async fn starting_a_topic_is_denied_while_genesis_needs_a_decision() {
    let fixture = fixture().await;
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO product_genesis_session (
            id, account_id, main_chat_id, prompt_revision, prompt_body, maturity,
            lifecycle, version, created_at, updated_at
         ) VALUES (?, ?, ?, 'v1', 'prompt body', 'mvp', 'ready_for_project', 1, ?, ?)",
    )
    .bind("topic-service-genesis")
    .bind(ACCOUNT_ID)
    .bind(&fixture.chat_id)
    .bind(&now)
    .bind(&now)
    .execute(fixture.db.pool())
    .await
    .expect("pending genesis session inserts");

    let error = fixture
        .topics
        .start_topic(StartMainChatTopicInput {
            actor_user_id: ACCOUNT_ID.to_owned(),
            chat_id: fixture.chat_id.clone(),
            label: Some("Should be blocked".to_owned()),
            summary: None,
        })
        .await
        .expect_err("a Genesis session awaiting finish/cancel denies the topic reset");
    assert!(matches!(error, ServiceError::Conflict(message) if message.contains("Genesis")));
}
