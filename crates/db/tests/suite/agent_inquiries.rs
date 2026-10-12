use db::{
    create_sqlite_pool, run_migrations, AgentChatRepo, AgentInquiryRepo, AgentInquiryStatus,
    CompleteAgentInquiry, CreateAgentInquiry, DbError, SqliteDb, User, UserRepo,
};

async fn database() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    SqliteDb::new(pool)
}

/// Every user gets an auto-created `account_main` Agent Chat (see
/// `V071__singular_agent_chats.sql`), which is all `agent_inquiry` needs for
/// its `chat_id` foreign key -- no project/agent/binding setup required.
async fn seed_chat(db: &SqliteDb, user_id: &str) -> String {
    let now = "2026-09-03T00:00:00.000Z".to_owned();
    UserRepo::create_user(
        db,
        &User {
            id: user_id.to_owned(),
            email: format!("{user_id}@example.test"),
            password_hash: "test".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("user creates");

    AgentChatRepo::get_main_chat(db, user_id)
        .await
        .expect("main chat lookup")
        .expect("main chat exists")
        .id
}

fn create_input(id: &str, chat_id: &str) -> CreateAgentInquiry {
    CreateAgentInquiry {
        id: id.to_owned(),
        chat_id: chat_id.to_owned(),
        turn_job_id: None,
        identity_id: "main-identity".to_owned(),
        owner_user_id: "inquiry-user".to_owned(),
        title: "Survey the auth flow".to_owned(),
        question: "How does session refresh work end to end?".to_owned(),
        workspace_path: None,
    }
}

#[tokio::test]
async fn create_and_get_roundtrip() {
    let db = database().await;
    let chat_id = seed_chat(&db, "inquiry-user").await;

    let created = AgentInquiryRepo::create_agent_inquiry(&db, create_input("inq-1", &chat_id))
        .await
        .expect("inquiry creates");

    assert_eq!(created.id, "inq-1");
    assert_eq!(created.chat_id, chat_id);
    assert_eq!(created.turn_job_id, None);
    assert_eq!(created.identity_id, "main-identity");
    assert_eq!(created.owner_user_id, "inquiry-user");
    assert_eq!(created.title, "Survey the auth flow");
    assert!(matches!(created.status, AgentInquiryStatus::Running));
    assert_eq!(created.findings, None);
    assert_eq!(created.findings_path, None);
    assert_eq!(created.workspace_path, None);
    assert_eq!(created.error, None);
    // The four token counters are disjoint and start at zero.
    assert_eq!(created.input_tokens, 0);
    assert_eq!(created.output_tokens, 0);
    assert_eq!(created.cache_read_tokens, 0);
    assert_eq!(created.cache_write_tokens, 0);
    assert_eq!(created.duration_ms, None);
    assert_eq!(created.version, 1);
    assert_eq!(created.finished_at, None);
    assert!(!created.created_at.is_empty());
    assert_eq!(created.created_at, created.updated_at);
    assert_eq!(created.created_at, created.started_at);

    let fetched = AgentInquiryRepo::get_agent_inquiry(&db, "inq-1")
        .await
        .expect("get succeeds")
        .expect("inquiry exists");
    assert_eq!(fetched, created);

    let missing = AgentInquiryRepo::get_agent_inquiry(&db, "does-not-exist")
        .await
        .expect("get succeeds");
    assert_eq!(missing, None);
}

#[tokio::test]
async fn list_paginates_newest_first_with_has_more() {
    let db = database().await;
    let chat_id = seed_chat(&db, "inquiry-user").await;

    for id in ["inq-a", "inq-b", "inq-c"] {
        AgentInquiryRepo::create_agent_inquiry(&db, create_input(id, &chat_id))
            .await
            .unwrap_or_else(|_| panic!("{id} creates"));
    }

    let first_page = AgentInquiryRepo::list_agent_inquiries(&db, &chat_id, 2, None)
        .await
        .expect("first page lists");
    assert_eq!(first_page.items.len(), 2);
    // Newest first: created last (inq-c) leads, then inq-b.
    assert_eq!(first_page.items[0].id, "inq-c");
    assert_eq!(first_page.items[1].id, "inq-b");
    assert!(first_page.next_cursor.is_some());

    let second_page =
        AgentInquiryRepo::list_agent_inquiries(&db, &chat_id, 2, first_page.next_cursor.as_deref())
            .await
            .expect("second page lists");
    assert_eq!(second_page.items.len(), 1);
    assert_eq!(second_page.items[0].id, "inq-a");
    assert_eq!(second_page.next_cursor, None);

    // A different chat's inquiries never leak into this chat's list.
    let other_chat_id = seed_chat(&db, "other-inquiry-user").await;
    AgentInquiryRepo::create_agent_inquiry(&db, create_input("inq-other", &other_chat_id))
        .await
        .expect("other chat inquiry creates");
    let scoped = AgentInquiryRepo::list_agent_inquiries(&db, &chat_id, 10, None)
        .await
        .expect("scoped list");
    assert_eq!(scoped.items.len(), 3);
    assert!(scoped.items.iter().all(|item| item.chat_id == chat_id));
}

#[tokio::test]
async fn complete_sets_terminal_status_and_disjoint_token_counters() {
    let db = database().await;
    let chat_id = seed_chat(&db, "inquiry-user").await;
    let created = AgentInquiryRepo::create_agent_inquiry(&db, create_input("inq-1", &chat_id))
        .await
        .expect("inquiry creates");

    let completed = AgentInquiryRepo::complete_agent_inquiry(
        &db,
        CompleteAgentInquiry {
            id: created.id.clone(),
            expected_version: created.version,
            status: AgentInquiryStatus::Succeeded,
            findings: Some("Session refresh rotates the token every 15 minutes.".to_owned()),
            findings_path: Some("findings/inq-1.md".to_owned()),
            error: None,
            input_tokens: 1200,
            output_tokens: 340,
            cache_read_tokens: 900,
            cache_write_tokens: 150,
            duration_ms: Some(4521),
        },
    )
    .await
    .expect("complete succeeds");

    assert!(matches!(completed.status, AgentInquiryStatus::Succeeded));
    assert_eq!(
        completed.findings,
        Some("Session refresh rotates the token every 15 minutes.".to_owned())
    );
    assert_eq!(
        completed.findings_path,
        Some("findings/inq-1.md".to_owned())
    );
    assert_eq!(completed.error, None);
    // Disjoint counters: never summed or collapsed into one another.
    assert_eq!(completed.input_tokens, 1200);
    assert_eq!(completed.output_tokens, 340);
    assert_eq!(completed.cache_read_tokens, 900);
    assert_eq!(completed.cache_write_tokens, 150);
    assert_eq!(completed.duration_ms, Some(4521));
    assert_eq!(completed.version, created.version + 1);
    assert!(completed.finished_at.is_some());
}

#[tokio::test]
async fn complete_rejects_a_stale_version() {
    let db = database().await;
    let chat_id = seed_chat(&db, "inquiry-user").await;
    let created = AgentInquiryRepo::create_agent_inquiry(&db, create_input("inq-1", &chat_id))
        .await
        .expect("inquiry creates");

    AgentInquiryRepo::complete_agent_inquiry(
        &db,
        CompleteAgentInquiry {
            id: created.id.clone(),
            expected_version: created.version,
            status: AgentInquiryStatus::Failed,
            findings: None,
            findings_path: None,
            error: Some("sub-agent workspace unavailable".to_owned()),
            input_tokens: 10,
            output_tokens: 5,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            duration_ms: Some(200),
        },
    )
    .await
    .expect("first complete succeeds");

    // Replaying with the now-stale version conflicts rather than silently
    // re-applying (and re-completing an already-terminal row).
    let stale = AgentInquiryRepo::complete_agent_inquiry(
        &db,
        CompleteAgentInquiry {
            id: created.id.clone(),
            expected_version: created.version,
            status: AgentInquiryStatus::Succeeded,
            findings: Some("should not land".to_owned()),
            findings_path: None,
            error: None,
            input_tokens: 1,
            output_tokens: 1,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            duration_ms: None,
        },
    )
    .await;
    assert!(matches!(stale, Err(DbError::VersionConflict)));

    // The first completion's outcome is untouched by the rejected replay.
    let current = AgentInquiryRepo::get_agent_inquiry(&db, &created.id)
        .await
        .expect("get succeeds")
        .expect("inquiry exists");
    assert!(matches!(current.status, AgentInquiryStatus::Failed));
    assert_eq!(
        current.error,
        Some("sub-agent workspace unavailable".to_owned())
    );
}

#[tokio::test]
async fn cancel_sets_terminal_status_and_rejects_a_second_cancel() {
    let db = database().await;
    let chat_id = seed_chat(&db, "inquiry-user").await;
    let created = AgentInquiryRepo::create_agent_inquiry(&db, create_input("inq-1", &chat_id))
        .await
        .expect("inquiry creates");

    let cancelled = AgentInquiryRepo::cancel_agent_inquiry(&db, &created.id, created.version)
        .await
        .expect("cancel succeeds");
    assert!(matches!(cancelled.status, AgentInquiryStatus::Cancelled));
    assert_eq!(cancelled.version, created.version + 1);
    assert!(cancelled.finished_at.is_some());

    // The only user verb is cancel -- an already-terminal inquiry cannot be
    // cancelled again, even with the now-current version.
    let second_cancel =
        AgentInquiryRepo::cancel_agent_inquiry(&db, &created.id, cancelled.version).await;
    assert!(matches!(second_cancel, Err(DbError::VersionConflict)));

    // A stale (pre-cancel) version is rejected the same way.
    let stale_cancel =
        AgentInquiryRepo::cancel_agent_inquiry(&db, &created.id, created.version).await;
    assert!(matches!(stale_cancel, Err(DbError::VersionConflict)));

    let missing = AgentInquiryRepo::cancel_agent_inquiry(&db, "does-not-exist", 1).await;
    assert!(matches!(missing, Err(DbError::NotFound)));
}
