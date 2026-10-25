//! Chat and inquiry usage migration scopes, telemetry, and replay.

use super::*;

#[tokio::test]
async fn v135_chat_and_inquiry_import_preserves_scopes_telemetry_and_provenance() {
    let migration_dir = unique_temp_path("v135-chat-inquiry-migrations");
    fs::create_dir_all(&migration_dir).expect("temp migration dir creates");
    copy_migrations_up_to(134, &migration_dir);

    let db_path = unique_temp_path("v135-chat-inquiry-db").with_extension("db");
    let url = format!("sqlite://{}", db_path.display());
    let pool = create_sqlite_pool(&url).await.expect("pool");
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("pre-V135 migrations apply");

    let now = "2026-09-07T01:00:00Z";
    let owner_id = "v135-chat-owner";
    let agent_id = "v135-chat-agent";
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES (?, 'chat@example.test', 'not-a-password', 'Chat owner', ?, ?)",
    )
    .bind(owner_id)
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("owner inserts");

    for (project_id, project_name, project_owner_id) in [
        ("chat-project", "Chat project", Some(owner_id)),
        ("chat-ownerless-project", "Ownerless chat project", None),
        (
            "chat-dangling-project",
            "Dangling-owner chat project",
            Some("missing-chat-owner"),
        ),
    ] {
        sqlx::query(
            "INSERT INTO project (
                id, name, settings, workflow_definition, owner_id, created_at, updated_at
             ) VALUES (?, ?, '{}', '{}', ?, ?, ?)",
        )
        .bind(project_id)
        .bind(project_name)
        .bind(project_owner_id)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .expect("project inserts");
        sqlx::query(
            "INSERT INTO repo (
                id, project_id, name, remote_url, local_path, work_mode,
                default_branch, created_at, updated_at
             ) VALUES (?, ?, ?, ?, NULL, 'direct_merge', 'main', ?, ?)",
        )
        .bind(format!("{project_id}-repo"))
        .bind(project_id)
        .bind(format!("{project_name} repo"))
        .bind(format!("https://example.test/{project_id}.git"))
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .expect("repo inserts");
    }

    sqlx::query(
        "INSERT INTO agent_identity (
            id, name, description, max_concurrent_tasks,
            heartbeat_interval_seconds, max_missed_heartbeats, status,
            is_default, paused, version, created_at, updated_at, owner_id, visibility
         ) VALUES (?, 'Chat agent', NULL, 1, 30, 3, 'idle', 0, 0, 1, ?, ?, ?, 'account')",
    )
    .bind(agent_id)
    .bind(now)
    .bind(now)
    .bind(owner_id)
    .execute(&pool)
    .await
    .expect("agent identity inserts");
    for (profile_id, provider, model) in [
        ("chat-profile", Some("openai"), Some("chat-model")),
        (
            "chat-current-profile",
            Some("other-provider"),
            Some("current-model"),
        ),
        (
            "chat-empty-provider",
            Some("   "),
            Some("empty-provider-model"),
        ),
    ] {
        sqlx::query(
            "INSERT INTO agent_profile (
                id, identity_id, backend_kind, executor_type, provider, model,
                capabilities_json, tool_policy_json, config_json, version,
                created_at, updated_at
             ) VALUES (?, ?, 'native', 'codex', ?, ?, '[]', '{}', '{}', 1, ?, ?)",
        )
        .bind(profile_id)
        .bind(agent_id)
        .bind(provider)
        .bind(model)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .expect("agent profile inserts");
    }
    sqlx::query(
        "UPDATE agent_identity SET selected_profile_id = 'chat-current-profile' WHERE id = ?",
    )
    .bind(agent_id)
    .execute(&pool)
    .await
    .expect("current profile selection");

    // V071's insert triggers create deterministic singleton Chats for every
    // account/Project. Replace those empty migration-created rows with stable
    // fixture IDs so every imported ledger key can be asserted exactly.
    sqlx::query("DELETE FROM agent_chat WHERE account_id = ? OR project_id IS NOT NULL")
        .bind(owner_id)
        .execute(&pool)
        .await
        .expect("empty synthetic Chat cleanup");

    for (chat_id, kind, account_id, project_id) in [
        (
            "chat-project-scope",
            "project",
            Some(owner_id),
            Some("chat-project"),
        ),
        (
            "chat-ownerless-scope",
            "project",
            Some(owner_id),
            Some("chat-ownerless-project"),
        ),
        (
            "chat-dangling-scope",
            "project",
            Some(owner_id),
            Some("chat-dangling-project"),
        ),
        ("chat-main-scope", "account_main", Some(owner_id), None),
    ] {
        sqlx::query(
            "INSERT INTO agent_chat (
                id, kind, account_id, project_id, status, created_at, updated_at
             ) VALUES (?, ?, ?, ?, 'ready', ?, ?)",
        )
        .bind(chat_id)
        .bind(kind)
        .bind(account_id)
        .bind(project_id)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .expect("Agent Chat inserts");
    }

    let valid_usage =
        r#"{"input_tokens":12,"output_tokens":34,"cache_read_tokens":5,"cache_write_tokens":6}"#;
    let zero_usage =
        r#"{"input_tokens":0,"output_tokens":0,"cache_read_tokens":0,"cache_write_tokens":0}"#;
    let partial_usage = r#"{"input_tokens":12}"#;
    let negative_usage =
        r#"{"input_tokens":-1,"output_tokens":2,"cache_read_tokens":0,"cache_write_tokens":0}"#;
    let wrong_type_usage =
        r#"{"input_tokens":1.5,"output_tokens":2,"cache_read_tokens":0,"cache_write_tokens":0}"#;
    let chat_messages = [
        (
            "chat-project-valid",
            "chat-project-scope",
            0_i64,
            "agent",
            Some(agent_id),
            Some("chat-model"),
            Some("chat-profile"),
            Some(valid_usage),
            "native",
            "2026-09-07T01:00:01Z",
        ),
        (
            "chat-project-zero",
            "chat-project-scope",
            1_i64,
            "agent",
            Some(agent_id),
            Some("chat-model"),
            Some("chat-profile"),
            Some(zero_usage),
            "native",
            "2026-09-07T01:00:02Z",
        ),
        (
            "chat-ownerless",
            "chat-ownerless-scope",
            0_i64,
            "agent",
            Some(agent_id),
            Some("chat-model"),
            Some("chat-profile"),
            Some(valid_usage),
            "native",
            "2026-09-07T01:00:03Z",
        ),
        (
            "chat-dangling-owner",
            "chat-dangling-scope",
            0_i64,
            "agent",
            Some(agent_id),
            Some("chat-model"),
            Some("chat-profile"),
            Some(valid_usage),
            "native",
            "2026-09-07T01:00:04Z",
        ),
        (
            "chat-main-trigger",
            "chat-main-scope",
            0_i64,
            "user",
            Some(owner_id),
            None,
            None,
            None,
            "native",
            "2026-09-07T01:00:04Z",
        ),
        (
            "chat-main-valid",
            "chat-main-scope",
            1_i64,
            "agent",
            Some(agent_id),
            Some("chat-model"),
            Some("chat-profile"),
            Some(valid_usage),
            "native",
            "2026-09-07T01:00:05Z",
        ),
        (
            "chat-main-null",
            "chat-main-scope",
            2_i64,
            "agent",
            Some(agent_id),
            Some("chat-model"),
            Some("chat-profile"),
            None,
            "native",
            "2026-09-07T01:00:06Z",
        ),
        (
            "chat-main-malformed",
            "chat-main-scope",
            3_i64,
            "agent",
            Some(agent_id),
            Some("chat-model"),
            Some("chat-profile"),
            Some("not-json"),
            "native",
            "2026-09-07T01:00:07Z",
        ),
        (
            "chat-main-partial",
            "chat-main-scope",
            4_i64,
            "agent",
            Some(agent_id),
            Some("chat-model"),
            Some("chat-profile"),
            Some(partial_usage),
            "native",
            "2026-09-07T01:00:08Z",
        ),
        (
            "chat-main-negative",
            "chat-main-scope",
            5_i64,
            "agent",
            Some(agent_id),
            Some("chat-model"),
            Some("chat-profile"),
            Some(negative_usage),
            "native",
            "2026-09-07T01:00:09Z",
        ),
        (
            "chat-main-wrong-type",
            "chat-main-scope",
            6_i64,
            "agent",
            Some(agent_id),
            Some("chat-model"),
            Some("chat-profile"),
            Some(wrong_type_usage),
            "native",
            "2026-09-07T01:00:10Z",
        ),
        (
            "chat-main-empty-model",
            "chat-main-scope",
            7_i64,
            "agent",
            Some(agent_id),
            Some("   "),
            Some("chat-profile"),
            Some(valid_usage),
            "native",
            "2026-09-07T01:00:11Z",
        ),
        (
            "chat-main-empty-provider",
            "chat-main-scope",
            8_i64,
            "agent",
            Some(agent_id),
            Some("empty-provider-model"),
            Some("chat-empty-provider"),
            Some(valid_usage),
            "native",
            "2026-09-07T01:00:12Z",
        ),
        (
            "chat-main-dangling-profile",
            "chat-main-scope",
            9_i64,
            "agent",
            Some("missing-chat-author"),
            Some("dangling-model"),
            Some("missing-chat-profile"),
            Some(valid_usage),
            "native",
            "2026-09-07T01:00:13Z",
        ),
        (
            "chat-genesis-proof",
            "chat-main-scope",
            11_i64,
            "agent",
            Some(agent_id),
            Some("chat-model"),
            Some("chat-profile"),
            Some(valid_usage),
            "native",
            "2026-09-07T01:00:14Z",
        ),
        (
            "chat-genesis-nonproof",
            "chat-main-scope",
            12_i64,
            "agent",
            Some(agent_id),
            Some("chat-model"),
            Some("chat-profile"),
            Some(valid_usage),
            "native",
            "2026-09-07T01:00:15Z",
        ),
        (
            "chat-main-handoff",
            "chat-main-scope",
            10_i64,
            "agent",
            Some(agent_id),
            Some("chat-model"),
            Some("chat-profile"),
            Some(valid_usage),
            "handoff",
            "2026-09-07T01:00:16Z",
        ),
    ];
    for (
        message_id,
        chat_id,
        sequence,
        author_type,
        author_id,
        model,
        profile_id,
        token_usage_json,
        source_type,
        occurred_at,
    ) in chat_messages
    {
        sqlx::query(
            "INSERT INTO agent_chat_message (
                id, chat_id, sequence, author_type, author_id, content, status,
                model, profile_id, token_usage_json, correlation_id, source_type, created_at
             ) VALUES (?, ?, ?, ?, ?, 'historical chat response', 'complete', ?, ?, ?, ?, ?, ?)",
        )
        .bind(message_id)
        .bind(chat_id)
        .bind(sequence)
        .bind(author_type)
        .bind(author_id)
        .bind(model)
        .bind(profile_id)
        .bind(token_usage_json)
        .bind(format!("correlation-{message_id}"))
        .bind(source_type)
        .bind(occurred_at)
        .execute(&pool)
        .await
        .expect("Agent Chat message inserts");
    }

    // Keep a separate pre-canonical Room response with telemetry. V071/V075
    // already quarantined this source; V135 must not consult it in addition to
    // the canonical Agent Chat message above.
    sqlx::query(
        "INSERT INTO legacy_room (
            id, scope_type, scope_id, owner_user_id, owning_project_id, title, status,
            responder_policy, default_responder_identity_id, history_policy, message_count,
            last_message_at, version, created_at, updated_at
         ) VALUES (
            'legacy-room-not-canonical', 'project', 'chat-project', ?, 'chat-project',
            'Legacy Room', 'active', 'explicit_identity', ?, 'project_members', 1,
            ?, 1, ?, ?
         )",
    )
    .bind(owner_id)
    .bind(agent_id)
    .bind(now)
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("legacy Room inserts");
    sqlx::query(
        "INSERT INTO legacy_room_message (
            id, room_id, author_type, author_id, addressed_identity_id,
            reply_to_message_id, content, content_guard_json, sensitivity, status,
            outcome, model, profile_id, session_id, token_usage_json, duration_ms,
            error, correlation_id, source_event_id, sequence, created_at
         ) VALUES (
            'legacy-room-message', 'legacy-room-not-canonical', 'agent', ?, NULL, NULL,
            'legacy Room response', '{}', 'internal', 'complete', NULL, 'chat-model',
            ?, 'legacy-room-session', ?, NULL, NULL, 'legacy-room-correlation', NULL, 0, ?
         )",
    )
    .bind(agent_id)
    .bind("chat-profile")
    .bind(valid_usage)
    .bind(now)
    .execute(&pool)
    .await
    .expect("legacy Room message inserts");

    sqlx::query(
        "INSERT INTO product_genesis_session (
            id, account_id, main_chat_id, prompt_revision, prompt_body,
            maturity, lifecycle, source_message_ids_json, created_at, updated_at
         ) VALUES (
            'chat-genesis-session', ?, 'chat-main-scope', 'v1',
            'discover a product', 'mvp', 'discovering', ?, ?, ?
         )",
    )
    .bind(owner_id)
    .bind(r#"["chat-genesis-proof"]"#)
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("Genesis session inserts");

    sqlx::query(
        "INSERT INTO agent_chat_turn_job (
            id, chat_id, triggering_message_id, responder_identity_id, profile_id,
            canonical_scope_type, canonical_scope_id, status, dedupe_key,
            response_message_id, correlation_id, created_at, updated_at
         ) VALUES (
            'chat-inquiry-turn', 'chat-main-scope', 'chat-main-trigger', ?, ?,
            'agent_chat', 'chat-main-scope', 'succeeded', 'chat-inquiry-dedupe',
            'chat-main-valid', 'chat-inquiry-correlation', ?, ?
         )",
    )
    .bind(agent_id)
    .bind("chat-profile")
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("Agent Chat turn job inserts");

    for (
        inquiry_id,
        turn_job_id,
        identity_id,
        inquiry_owner_id,
        status,
        input_tokens,
        output_tokens,
        cache_read_tokens,
        cache_write_tokens,
        finished_at,
    ) in [
        (
            "inquiry-positive",
            Some("chat-inquiry-turn"),
            agent_id,
            owner_id,
            "succeeded",
            11_i64,
            22_i64,
            3_i64,
            4_i64,
            Some("2026-09-07T01:00:20Z"),
        ),
        (
            "inquiry-zero",
            None,
            agent_id,
            owner_id,
            "succeeded",
            0_i64,
            0_i64,
            0_i64,
            0_i64,
            Some("2026-09-07T01:00:21Z"),
        ),
    ] {
        sqlx::query(
            "INSERT INTO agent_inquiry (
                id, chat_id, turn_job_id, identity_id, owner_user_id, title, question,
                status, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
                version, created_at, updated_at, started_at, finished_at
             ) VALUES (?, 'chat-main-scope', ?, ?, ?, 'Historical inquiry', 'What happened?',
                       ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)",
        )
        .bind(inquiry_id)
        .bind(turn_job_id)
        .bind(identity_id)
        .bind(inquiry_owner_id)
        .bind(status)
        .bind(input_tokens)
        .bind(output_tokens)
        .bind(cache_read_tokens)
        .bind(cache_write_tokens)
        .bind(now)
        .bind(now)
        .bind(now)
        .bind(finished_at)
        .execute(&pool)
        .await
        .expect("inquiry inserts");
    }
    sqlx::query(
        "INSERT INTO agent_inquiry (
            id, chat_id, turn_job_id, identity_id, owner_user_id, title, question,
            status, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
            version, created_at, updated_at, started_at, finished_at
         ) VALUES (
            'inquiry-malformed', 'chat-main-scope', NULL, 'missing-inquiry-agent',
            'missing-inquiry-owner', 'Malformed inquiry', 'Malformed counters', 'failed',
            1.5, 'not-an-integer', -2, 0, 1, ?, ?, ?, ?
         )",
    )
    .bind(now)
    .bind(now)
    .bind(now)
    .bind("2026-09-07T01:00:22Z")
    .execute(&pool)
    .await
    .expect("malformed inquiry inserts");
    sqlx::query(
        "INSERT INTO agent_inquiry (
            id, chat_id, turn_job_id, identity_id, owner_user_id, title, question,
            status, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
            version, created_at, updated_at, started_at, finished_at
         ) VALUES (
            'inquiry-unfinished', 'chat-main-scope', NULL, ?, ?, 'Unfinished inquiry',
            'Must not import', 'running', 9, 9, 0, 0, 1, ?, ?, ?, NULL
         )",
    )
    .bind(agent_id)
    .bind(owner_id)
    .bind(now)
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("unfinished inquiry inserts");

    // Snapshot the fully populated V134 fixture before V135 so replay is
    // exercised from a fresh database, not only through the migration marker.
    let replay_db_path = unique_temp_path("v135-chat-inquiry-replay-db").with_extension("db");
    sqlx::query("VACUUM INTO ?")
        .bind(replay_db_path.to_string_lossy().to_string())
        .execute(&pool)
        .await
        .expect("pre-V135 fixture snapshot");

    run_migrations(&pool).await.expect("V135 migration applies");

    let canonical_chat_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_chat_message
         WHERE author_type = 'agent' AND status IN ('complete', 'failed', 'cancelled')
           AND source_type != 'handoff'",
    )
    .fetch_one(&pool)
    .await
    .expect("canonical chat source count");
    assert_eq!(canonical_chat_count, 15);
    let chat_selection_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pricing_selection WHERE provenance_kind = 'legacy_chat'",
    )
    .fetch_one(&pool)
    .await
    .expect("chat selection count");
    let chat_invocation_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM usage_invocation WHERE provenance_kind = 'legacy_chat'",
    )
    .fetch_one(&pool)
    .await
    .expect("chat invocation count");
    let chat_event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM usage_event
         WHERE provenance_kind = 'legacy_chat' AND report_mode = 'legacy_aggregate'",
    )
    .fetch_one(&pool)
    .await
    .expect("chat event count");
    assert_eq!(chat_selection_count, canonical_chat_count);
    assert_eq!(chat_invocation_count, canonical_chat_count);
    assert_eq!(chat_event_count, 14);

    type ExpectedChat<'a> = (
        &'a str,
        &'a str,
        Option<&'a str>,
        Option<&'a str>,
        Option<&'a str>,
        Option<&'a str>,
        Option<&'a str>,
        Option<&'a str>,
        Option<&'a str>,
        &'a str,
        Option<&'a str>,
        [Option<i64>; 4],
        bool,
        &'a str,
    );
    let expected_chats: &[ExpectedChat<'_>] = &[
        (
            "chat-project-valid",
            "project_chat",
            Some("chat-project"),
            Some(owner_id),
            Some("openai"),
            Some("chat-model"),
            Some("chat-model"),
            Some(agent_id),
            Some("chat-profile"),
            "metered",
            None,
            [Some(12), Some(34), Some(5), Some(6)],
            true,
            "2026-09-07T01:00:01Z",
        ),
        (
            "chat-project-zero",
            "project_chat",
            Some("chat-project"),
            Some(owner_id),
            Some("openai"),
            Some("chat-model"),
            Some("chat-model"),
            Some(agent_id),
            Some("chat-profile"),
            "metered",
            None,
            [Some(0), Some(0), Some(0), Some(0)],
            true,
            "2026-09-07T01:00:02Z",
        ),
        (
            "chat-ownerless",
            "project_chat",
            Some("chat-ownerless-project"),
            None,
            Some("openai"),
            Some("chat-model"),
            Some("chat-model"),
            Some(agent_id),
            Some("chat-profile"),
            "metered",
            None,
            [Some(12), Some(34), Some(5), Some(6)],
            true,
            "2026-09-07T01:00:03Z",
        ),
        (
            "chat-dangling-owner",
            "project_chat",
            Some("chat-dangling-project"),
            None,
            Some("openai"),
            Some("chat-model"),
            Some("chat-model"),
            Some(agent_id),
            Some("chat-profile"),
            "metered",
            None,
            [Some(12), Some(34), Some(5), Some(6)],
            true,
            "2026-09-07T01:00:04Z",
        ),
        (
            "chat-main-valid",
            "main_chat",
            None,
            Some(owner_id),
            Some("openai"),
            Some("chat-model"),
            Some("chat-model"),
            Some(agent_id),
            Some("chat-profile"),
            "metered",
            None,
            [Some(12), Some(34), Some(5), Some(6)],
            true,
            "2026-09-07T01:00:05Z",
        ),
        (
            "chat-main-null",
            "main_chat",
            None,
            Some(owner_id),
            Some("openai"),
            Some("chat-model"),
            Some("chat-model"),
            Some(agent_id),
            Some("chat-profile"),
            "unmetered",
            Some("legacy chat response has no token telemetry"),
            [None, None, None, None],
            false,
            "2026-09-07T01:00:06Z",
        ),
        (
            "chat-main-malformed",
            "main_chat",
            None,
            Some(owner_id),
            Some("openai"),
            Some("chat-model"),
            Some("chat-model"),
            Some(agent_id),
            Some("chat-profile"),
            "unmetered",
            Some("legacy chat row retained with invalid telemetry"),
            [None, None, None, None],
            true,
            "2026-09-07T01:00:07Z",
        ),
        (
            "chat-main-partial",
            "main_chat",
            None,
            Some(owner_id),
            Some("openai"),
            Some("chat-model"),
            Some("chat-model"),
            Some(agent_id),
            Some("chat-profile"),
            "unmetered",
            Some("legacy chat row retained with invalid telemetry"),
            [None, None, None, None],
            true,
            "2026-09-07T01:00:08Z",
        ),
        (
            "chat-main-negative",
            "main_chat",
            None,
            Some(owner_id),
            Some("openai"),
            Some("chat-model"),
            Some("chat-model"),
            Some(agent_id),
            Some("chat-profile"),
            "unmetered",
            Some("legacy chat row retained with invalid telemetry"),
            [None, None, None, None],
            true,
            "2026-09-07T01:00:09Z",
        ),
        (
            "chat-main-wrong-type",
            "main_chat",
            None,
            Some(owner_id),
            Some("openai"),
            Some("chat-model"),
            Some("chat-model"),
            Some(agent_id),
            Some("chat-profile"),
            "unmetered",
            Some("legacy chat row retained with invalid telemetry"),
            [None, None, None, None],
            true,
            "2026-09-07T01:00:10Z",
        ),
        (
            "chat-main-empty-model",
            "main_chat",
            None,
            Some(owner_id),
            None,
            None,
            Some("   "),
            Some(agent_id),
            Some("chat-profile"),
            "metered",
            None,
            [Some(12), Some(34), Some(5), Some(6)],
            true,
            "2026-09-07T01:00:11Z",
        ),
        (
            "chat-main-empty-provider",
            "main_chat",
            None,
            Some(owner_id),
            None,
            Some("empty-provider-model"),
            Some("empty-provider-model"),
            Some(agent_id),
            Some("chat-empty-provider"),
            "metered",
            None,
            [Some(12), Some(34), Some(5), Some(6)],
            true,
            "2026-09-07T01:00:12Z",
        ),
        (
            "chat-main-dangling-profile",
            "main_chat",
            None,
            Some(owner_id),
            None,
            Some("dangling-model"),
            Some("dangling-model"),
            Some("missing-chat-author"),
            Some("missing-chat-profile"),
            "metered",
            None,
            [Some(12), Some(34), Some(5), Some(6)],
            true,
            "2026-09-07T01:00:13Z",
        ),
        (
            "chat-genesis-proof",
            "genesis_chat",
            None,
            Some(owner_id),
            Some("openai"),
            Some("chat-model"),
            Some("chat-model"),
            Some(agent_id),
            Some("chat-profile"),
            "metered",
            None,
            [Some(12), Some(34), Some(5), Some(6)],
            true,
            "2026-09-07T01:00:14Z",
        ),
        (
            "chat-genesis-nonproof",
            "main_chat",
            None,
            Some(owner_id),
            Some("openai"),
            Some("chat-model"),
            Some("chat-model"),
            Some(agent_id),
            Some("chat-profile"),
            "metered",
            None,
            [Some(12), Some(34), Some(5), Some(6)],
            true,
            "2026-09-07T01:00:15Z",
        ),
    ];

    for (
        source_id,
        expected_surface,
        expected_project,
        expected_owner,
        expected_provider,
        expected_model,
        expected_raw_model,
        expected_agent,
        expected_profile,
        expected_telemetry,
        expected_terminal_reason,
        expected_counters,
        has_event,
        occurred_at,
    ) in expected_chats
    {
        let selection_id = format!("legacy-chat-selection:{source_id}");
        let invocation_id = format!("legacy-chat-invocation:{source_id}");
        let selection = sqlx::query(
            "SELECT id, invocation_id, domain_kind, surface, source_id, execution_id,
                    task_id, owner_user_id, project_id, admitted_provider_id,
                    admitted_model_id, runtime_model, selection_status, selected_at
             FROM pricing_selection WHERE id = ?",
        )
        .bind(&selection_id)
        .fetch_one(&pool)
        .await
        .expect("chat selection lookup");
        assert_eq!(selection.get::<String, _>("id"), selection_id);
        assert_eq!(
            selection
                .get::<Option<String>, _>("invocation_id")
                .as_deref(),
            Some(invocation_id.as_str())
        );
        assert_eq!(selection.get::<String, _>("domain_kind"), "chat");
        assert_eq!(selection.get::<String, _>("surface"), *expected_surface);
        assert_eq!(selection.get::<String, _>("source_id"), *source_id);
        assert_eq!(selection.get::<Option<String>, _>("execution_id"), None);
        assert_eq!(selection.get::<Option<String>, _>("task_id"), None);
        assert_eq!(
            selection
                .get::<Option<String>, _>("owner_user_id")
                .as_deref(),
            *expected_owner
        );
        assert_eq!(
            selection.get::<Option<String>, _>("project_id").as_deref(),
            *expected_project
        );
        assert_eq!(
            selection
                .get::<Option<String>, _>("admitted_provider_id")
                .as_deref(),
            *expected_provider
        );
        assert_eq!(
            selection
                .get::<Option<String>, _>("admitted_model_id")
                .as_deref(),
            *expected_model
        );
        assert_eq!(
            selection
                .get::<Option<String>, _>("runtime_model")
                .as_deref(),
            *expected_model
        );
        assert_eq!(selection.get::<String, _>("selection_status"), "unpriced");
        assert_eq!(selection.get::<String, _>("selected_at"), *occurred_at);

        let invocation = sqlx::query(
            "SELECT id, pricing_selection_id, domain_kind, surface, source_id,
                    lifecycle, telemetry_state, terminal_reason, owner_user_id,
                    project_id, admitted_provider_id, admitted_model_id,
                    admitted_runtime_model, agent_id, profile_id, admitted_at,
                    started_at, settled_at
             FROM usage_invocation WHERE id = ?",
        )
        .bind(&invocation_id)
        .fetch_one(&pool)
        .await
        .expect("chat invocation lookup");
        assert_eq!(invocation.get::<String, _>("id"), invocation_id);
        assert_eq!(
            invocation
                .get::<Option<String>, _>("pricing_selection_id")
                .as_deref(),
            Some(selection_id.as_str())
        );
        assert_eq!(invocation.get::<String, _>("domain_kind"), "chat");
        assert_eq!(invocation.get::<String, _>("surface"), *expected_surface);
        assert_eq!(invocation.get::<String, _>("source_id"), *source_id);
        assert_eq!(invocation.get::<String, _>("lifecycle"), "settled");
        assert_eq!(
            invocation.get::<String, _>("telemetry_state"),
            *expected_telemetry
        );
        assert_eq!(
            invocation
                .get::<Option<String>, _>("terminal_reason")
                .as_deref(),
            *expected_terminal_reason
        );
        assert_eq!(
            invocation
                .get::<Option<String>, _>("owner_user_id")
                .as_deref(),
            *expected_owner
        );
        assert_eq!(
            invocation.get::<Option<String>, _>("project_id").as_deref(),
            *expected_project
        );
        assert_eq!(
            invocation
                .get::<Option<String>, _>("admitted_provider_id")
                .as_deref(),
            *expected_provider
        );
        assert_eq!(
            invocation
                .get::<Option<String>, _>("admitted_model_id")
                .as_deref(),
            *expected_model
        );
        assert_eq!(
            invocation
                .get::<Option<String>, _>("admitted_runtime_model")
                .as_deref(),
            *expected_model
        );
        assert_eq!(
            invocation.get::<Option<String>, _>("agent_id").as_deref(),
            *expected_agent
        );
        assert_eq!(
            invocation.get::<Option<String>, _>("profile_id").as_deref(),
            *expected_profile
        );
        assert_eq!(invocation.get::<String, _>("admitted_at"), *occurred_at);
        assert_eq!(
            invocation.get::<Option<String>, _>("started_at").as_deref(),
            Some(*occurred_at)
        );
        assert_eq!(
            invocation.get::<Option<String>, _>("settled_at").as_deref(),
            Some(*occurred_at)
        );

        if !*has_event {
            let event_count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM usage_event WHERE id = ?")
                    .bind(format!("legacy-chat-event:{source_id}"))
                    .fetch_one(&pool)
                    .await
                    .expect("NULL chat event absence lookup");
            assert_eq!(event_count, 0, "NULL chat telemetry has no event");
            continue;
        }

        let event_id = format!("legacy-chat-event:{source_id}");
        let event = sqlx::query(
            "SELECT invocation_id, owner_user_id, project_id, surface, source_id,
                    source_report_id, legacy_source_table, legacy_source_id,
                    legacy_provider_raw, legacy_provider_sqlite_type,
                    legacy_provider_sql_literal, legacy_model_raw,
                    legacy_model_sqlite_type, legacy_model_sql_literal,
                    legacy_counter_values_json, legacy_cost_usd_raw,
                    legacy_created_at_raw, legacy_project_owner_raw,
                    legacy_invalid_usage, provider_id, model_id, runtime_model,
                    agent_id, profile_id, agent_name_snapshot, project_name_snapshot,
                    executor_type, telemetry_state, input_tokens, output_tokens,
                    cache_read_tokens, cache_write_tokens, provider_reported_nano_usd,
                    legacy_reported_cost_usd, cost_kind, coverage_reason_code,
                    occurred_at, created_at
             FROM usage_event WHERE id = ?",
        )
        .bind(&event_id)
        .fetch_one(&pool)
        .await
        .expect("chat event lookup");
        assert_eq!(event.get::<String, _>("invocation_id"), invocation_id);
        assert_eq!(
            event.get::<Option<String>, _>("owner_user_id").as_deref(),
            *expected_owner
        );
        assert_eq!(
            event.get::<Option<String>, _>("project_id").as_deref(),
            *expected_project
        );
        assert_eq!(event.get::<String, _>("surface"), *expected_surface);
        assert_eq!(event.get::<String, _>("source_id"), *source_id);
        assert_eq!(
            event.get::<String, _>("source_report_id"),
            format!("legacy-chat-report:{source_id}")
        );
        assert_eq!(
            event.get::<String, _>("legacy_source_table"),
            "agent_chat_message"
        );
        assert_eq!(event.get::<String, _>("legacy_source_id"), *source_id);
        assert_eq!(event.get::<Option<String>, _>("legacy_provider_raw"), None);
        assert_eq!(
            event.get::<String, _>("legacy_provider_sqlite_type"),
            "absent"
        );
        assert_eq!(
            event.get::<String, _>("legacy_provider_sql_literal"),
            "NULL"
        );
        assert_eq!(
            event
                .get::<Option<String>, _>("legacy_model_raw")
                .as_deref(),
            *expected_raw_model
        );
        assert_eq!(event.get::<String, _>("legacy_model_sqlite_type"), "text");
        assert_eq!(
            event.get::<String, _>("legacy_model_sql_literal"),
            format!("'{}'", expected_raw_model.expect("chat model raw value"))
        );
        assert_eq!(event.get::<String, _>("legacy_cost_usd_raw"), "NULL");
        assert_eq!(
            event.get::<String, _>("legacy_created_at_raw"),
            format!("'{occurred_at}'")
        );
        let expected_project_owner_raw = match expected_project {
            Some("chat-project") => format!("'{owner_id}'"),
            Some("chat-ownerless-project") => "NULL".to_owned(),
            Some("chat-dangling-project") => "'missing-chat-owner'".to_owned(),
            None => format!("'{owner_id}'"),
            _ => unreachable!(),
        };
        assert_eq!(
            event.get::<String, _>("legacy_project_owner_raw"),
            expected_project_owner_raw
        );
        let expected_invalid = i64::from(*expected_telemetry == "unmetered");
        assert_eq!(
            event.get::<i64, _>("legacy_invalid_usage"),
            expected_invalid
        );
        assert_eq!(
            event.get::<Option<String>, _>("provider_id").as_deref(),
            *expected_provider
        );
        assert_eq!(
            event.get::<Option<String>, _>("model_id").as_deref(),
            *expected_model
        );
        assert_eq!(
            event.get::<Option<String>, _>("runtime_model").as_deref(),
            *expected_model
        );
        assert_eq!(
            event.get::<Option<String>, _>("agent_id").as_deref(),
            *expected_agent
        );
        assert_eq!(
            event.get::<Option<String>, _>("profile_id").as_deref(),
            *expected_profile
        );
        assert_eq!(event.get::<Option<String>, _>("agent_name_snapshot"), None);
        assert_eq!(
            event.get::<Option<String>, _>("project_name_snapshot"),
            None
        );
        let expected_executor = if matches!(
            (*expected_profile, *expected_raw_model),
            (Some("chat-profile"), Some("chat-model"))
                | (Some("chat-empty-provider"), Some("empty-provider-model"))
        ) {
            Some("codex")
        } else {
            None
        };
        assert_eq!(
            event.get::<Option<String>, _>("executor_type").as_deref(),
            expected_executor
        );
        assert_eq!(
            event.get::<String, _>("telemetry_state"),
            *expected_telemetry
        );
        assert_eq!(
            [
                event.get::<Option<i64>, _>("input_tokens"),
                event.get::<Option<i64>, _>("output_tokens"),
                event.get::<Option<i64>, _>("cache_read_tokens"),
                event.get::<Option<i64>, _>("cache_write_tokens"),
            ],
            *expected_counters
        );
        assert_eq!(
            event.get::<Option<i64>, _>("provider_reported_nano_usd"),
            None
        );
        assert_eq!(
            event.get::<Option<f64>, _>("legacy_reported_cost_usd"),
            None
        );
        assert_eq!(event.get::<String, _>("cost_kind"), "none");
        let expected_reason = if *expected_telemetry == "unmetered" {
            "invalid_legacy_usage"
        } else if expected_provider.is_none() {
            "missing_provider"
        } else if expected_model.is_none() {
            "missing_model"
        } else {
            "missing_rate"
        };
        assert_eq!(
            event
                .get::<Option<String>, _>("coverage_reason_code")
                .as_deref(),
            Some(expected_reason)
        );
        assert_eq!(event.get::<String, _>("occurred_at"), *occurred_at);
        assert_eq!(event.get::<String, _>("created_at"), *occurred_at);

        let metadata = event.get::<String, _>("legacy_counter_values_json");
        let metadata_source: Option<String> =
            sqlx::query_scalar("SELECT json_extract(?, '$.source_table')")
                .bind(&metadata)
                .fetch_one(&pool)
                .await
                .expect("chat telemetry source metadata");
        assert_eq!(metadata_source.as_deref(), Some("agent_chat_message"));
        let metadata_json_valid: Option<i64> =
            sqlx::query_scalar("SELECT json_extract(?, '$.json_valid')")
                .bind(&metadata)
                .fetch_one(&pool)
                .await
                .expect("chat telemetry validity metadata");
        assert_eq!(
            metadata_json_valid,
            Some(i64::from(*source_id != "chat-main-malformed"))
        );
        if *expected_telemetry == "metered" {
            let metadata_input: Option<i64> =
                sqlx::query_scalar("SELECT json_extract(?, '$.input_tokens.value')")
                    .bind(&metadata)
                    .fetch_one(&pool)
                    .await
                    .expect("chat input metadata");
            assert_eq!(metadata_input, expected_counters[0]);
        }
    }

    let handoff_import_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM usage_invocation WHERE source_id = 'chat-main-handoff'",
    )
    .fetch_one(&pool)
    .await
    .expect("handoff source exclusion lookup");
    assert_eq!(handoff_import_count, 0);

    let inquiry_source_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_inquiry
         WHERE status IN ('succeeded', 'failed', 'cancelled') AND finished_at IS NOT NULL
           AND length(trim(CAST(finished_at AS TEXT))) > 0",
    )
    .fetch_one(&pool)
    .await
    .expect("inquiry source count");
    assert_eq!(inquiry_source_count, 3);
    for (table, expected_count) in [
        ("pricing_selection", inquiry_source_count),
        ("usage_invocation", inquiry_source_count),
        ("usage_event", inquiry_source_count),
    ] {
        let count: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM {table} WHERE provenance_kind = 'legacy_inquiry'",
        ))
        .fetch_one(&pool)
        .await
        .expect("inquiry target count");
        assert_eq!(
            count, expected_count,
            "one {table} row per terminal inquiry"
        );
    }
    let unfinished_import_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM usage_invocation WHERE source_id = 'inquiry-unfinished'",
    )
    .fetch_one(&pool)
    .await
    .expect("unfinished inquiry exclusion lookup");
    assert_eq!(unfinished_import_count, 0);

    type ExpectedInquiry<'a> = (
        &'a str,
        Option<&'a str>,
        Option<&'a str>,
        Option<&'a str>,
        &'a str,
        Option<&'a str>,
        Option<&'a str>,
        Option<&'a str>,
        &'a str,
        [Option<i64>; 4],
        &'a str,
    );
    let expected_inquiries: &[ExpectedInquiry<'_>] = &[
        (
            "inquiry-positive",
            Some(owner_id),
            Some(agent_id),
            Some("chat-profile"),
            "metered",
            Some("openai"),
            Some("chat-model"),
            None,
            "missing_rate",
            [Some(11), Some(22), Some(3), Some(4)],
            "2026-09-07T01:00:20Z",
        ),
        (
            "inquiry-zero",
            Some(owner_id),
            Some(agent_id),
            None,
            "unmetered",
            None,
            None,
            Some("legacy inquiry counters are unknown without a reported marker"),
            "unmetered",
            [None, None, None, None],
            "2026-09-07T01:00:21Z",
        ),
        (
            "inquiry-malformed",
            None,
            Some("missing-inquiry-agent"),
            None,
            "unmetered",
            None,
            None,
            Some("legacy inquiry row retained with invalid telemetry"),
            "invalid_legacy_usage",
            [None, None, None, None],
            "2026-09-07T01:00:22Z",
        ),
    ];
    for (
        source_id,
        expected_owner,
        expected_agent,
        expected_profile,
        expected_telemetry,
        expected_provider,
        expected_model,
        expected_terminal_reason,
        expected_reason,
        expected_counters,
        occurred_at,
    ) in expected_inquiries
    {
        let selection_id = format!("legacy-inquiry-selection:{source_id}");
        let invocation_id = format!("legacy-inquiry-invocation:{source_id}");
        let selection = sqlx::query(
            "SELECT id, invocation_id, domain_kind, surface, source_id, owner_user_id,
                    project_id, admitted_provider_id, admitted_model_id, runtime_model,
                    selection_status, selected_at
             FROM pricing_selection WHERE id = ?",
        )
        .bind(&selection_id)
        .fetch_one(&pool)
        .await
        .expect("inquiry selection lookup");
        assert_eq!(selection.get::<String, _>("id"), selection_id);
        assert_eq!(
            selection
                .get::<Option<String>, _>("invocation_id")
                .as_deref(),
            Some(invocation_id.as_str())
        );
        assert_eq!(selection.get::<String, _>("domain_kind"), "inquiry");
        assert_eq!(selection.get::<String, _>("surface"), "main_inquiry");
        assert_eq!(selection.get::<String, _>("source_id"), *source_id);
        assert_eq!(
            selection
                .get::<Option<String>, _>("owner_user_id")
                .as_deref(),
            *expected_owner
        );
        assert_eq!(selection.get::<Option<String>, _>("project_id"), None);
        assert_eq!(
            selection
                .get::<Option<String>, _>("admitted_provider_id")
                .as_deref(),
            *expected_provider
        );
        assert_eq!(
            selection
                .get::<Option<String>, _>("admitted_model_id")
                .as_deref(),
            *expected_model
        );
        assert_eq!(
            selection
                .get::<Option<String>, _>("runtime_model")
                .as_deref(),
            *expected_model
        );
        assert_eq!(selection.get::<String, _>("selection_status"), "unpriced");
        assert_eq!(selection.get::<String, _>("selected_at"), *occurred_at);

        let invocation = sqlx::query(
            "SELECT id, pricing_selection_id, surface, source_id, lifecycle,
                    telemetry_state, terminal_reason, owner_user_id, project_id,
                    admitted_provider_id, admitted_model_id, admitted_runtime_model,
                    agent_id, profile_id, admitted_at, started_at, settled_at
             FROM usage_invocation WHERE id = ?",
        )
        .bind(&invocation_id)
        .fetch_one(&pool)
        .await
        .expect("inquiry invocation lookup");
        assert_eq!(invocation.get::<String, _>("id"), invocation_id);
        assert_eq!(
            invocation
                .get::<Option<String>, _>("pricing_selection_id")
                .as_deref(),
            Some(selection_id.as_str())
        );
        assert_eq!(invocation.get::<String, _>("surface"), "main_inquiry");
        assert_eq!(invocation.get::<String, _>("source_id"), *source_id);
        assert_eq!(invocation.get::<String, _>("lifecycle"), "settled");
        assert_eq!(
            invocation.get::<String, _>("telemetry_state"),
            *expected_telemetry
        );
        assert_eq!(
            invocation
                .get::<Option<String>, _>("terminal_reason")
                .as_deref(),
            *expected_terminal_reason
        );
        assert_eq!(
            invocation
                .get::<Option<String>, _>("owner_user_id")
                .as_deref(),
            *expected_owner
        );
        assert_eq!(invocation.get::<Option<String>, _>("project_id"), None);
        assert_eq!(
            invocation
                .get::<Option<String>, _>("admitted_provider_id")
                .as_deref(),
            *expected_provider
        );
        assert_eq!(
            invocation
                .get::<Option<String>, _>("admitted_model_id")
                .as_deref(),
            *expected_model
        );
        assert_eq!(
            invocation
                .get::<Option<String>, _>("admitted_runtime_model")
                .as_deref(),
            *expected_model
        );
        assert_eq!(
            invocation.get::<Option<String>, _>("agent_id").as_deref(),
            *expected_agent
        );
        assert_eq!(
            invocation.get::<Option<String>, _>("profile_id").as_deref(),
            *expected_profile
        );
        assert_eq!(invocation.get::<String, _>("admitted_at"), *occurred_at);
        assert_eq!(
            invocation.get::<Option<String>, _>("started_at").as_deref(),
            Some(*occurred_at)
        );
        assert_eq!(
            invocation.get::<Option<String>, _>("settled_at").as_deref(),
            Some(*occurred_at)
        );

        let event = sqlx::query(
            "SELECT invocation_id, owner_user_id, project_id, surface, source_id,
                    source_report_id, legacy_source_table, legacy_source_id,
                    legacy_provider_raw, legacy_provider_sqlite_type,
                    legacy_provider_sql_literal, legacy_model_raw,
                    legacy_model_sqlite_type, legacy_model_sql_literal,
                    legacy_counter_values_json, legacy_cost_usd_raw,
                    legacy_created_at_raw, legacy_project_owner_raw,
                    legacy_invalid_usage, provider_id, model_id, runtime_model,
                    agent_id, profile_id, agent_name_snapshot, project_name_snapshot,
                    executor_type, telemetry_state, input_tokens, output_tokens,
                    cache_read_tokens, cache_write_tokens, cost_kind,
                    coverage_reason_code, occurred_at, created_at
             FROM usage_event WHERE id = ?",
        )
        .bind(format!("legacy-inquiry-event:{source_id}"))
        .fetch_one(&pool)
        .await
        .expect("inquiry event lookup");
        assert_eq!(event.get::<String, _>("invocation_id"), invocation_id);
        assert_eq!(
            event.get::<Option<String>, _>("owner_user_id").as_deref(),
            *expected_owner
        );
        assert_eq!(event.get::<Option<String>, _>("project_id"), None);
        assert_eq!(event.get::<String, _>("surface"), "main_inquiry");
        assert_eq!(event.get::<String, _>("source_id"), *source_id);
        assert_eq!(
            event.get::<String, _>("source_report_id"),
            format!("legacy-inquiry-report:{source_id}")
        );
        assert_eq!(
            event.get::<String, _>("legacy_source_table"),
            "agent_inquiry"
        );
        assert_eq!(event.get::<String, _>("legacy_source_id"), *source_id);
        assert_eq!(event.get::<Option<String>, _>("legacy_provider_raw"), None);
        assert_eq!(
            event.get::<String, _>("legacy_provider_sqlite_type"),
            "absent"
        );
        assert_eq!(
            event.get::<String, _>("legacy_provider_sql_literal"),
            "NULL"
        );
        assert_eq!(event.get::<Option<String>, _>("legacy_model_raw"), None);
        assert_eq!(event.get::<String, _>("legacy_model_sqlite_type"), "absent");
        assert_eq!(event.get::<String, _>("legacy_model_sql_literal"), "NULL");
        assert_eq!(event.get::<String, _>("legacy_cost_usd_raw"), "NULL");
        assert_eq!(
            event.get::<String, _>("legacy_created_at_raw"),
            format!("'{occurred_at}'")
        );
        assert_eq!(
            event.get::<String, _>("legacy_project_owner_raw"),
            match expected_owner {
                Some(_) => format!("'{expected_owner_id}'", expected_owner_id = owner_id),
                None => "'missing-inquiry-owner'".to_owned(),
            }
        );
        assert_eq!(
            event.get::<i64, _>("legacy_invalid_usage"),
            i64::from(*expected_reason == "invalid_legacy_usage")
        );
        assert_eq!(
            event.get::<Option<String>, _>("provider_id").as_deref(),
            *expected_provider
        );
        assert_eq!(
            event.get::<Option<String>, _>("model_id").as_deref(),
            *expected_model
        );
        assert_eq!(
            event.get::<Option<String>, _>("runtime_model").as_deref(),
            *expected_model
        );
        assert_eq!(
            event.get::<Option<String>, _>("agent_id").as_deref(),
            *expected_agent
        );
        assert_eq!(
            event.get::<Option<String>, _>("profile_id").as_deref(),
            *expected_profile
        );
        assert_eq!(event.get::<Option<String>, _>("agent_name_snapshot"), None);
        assert_eq!(
            event.get::<Option<String>, _>("project_name_snapshot"),
            None
        );
        assert_eq!(
            event.get::<Option<String>, _>("executor_type").as_deref(),
            (*expected_profile == Some("chat-profile")).then_some("codex")
        );
        assert_eq!(
            event.get::<String, _>("telemetry_state"),
            *expected_telemetry
        );
        assert_eq!(
            [
                event.get::<Option<i64>, _>("input_tokens"),
                event.get::<Option<i64>, _>("output_tokens"),
                event.get::<Option<i64>, _>("cache_read_tokens"),
                event.get::<Option<i64>, _>("cache_write_tokens"),
            ],
            *expected_counters
        );
        assert_eq!(event.get::<String, _>("cost_kind"), "none");
        assert_eq!(
            event
                .get::<Option<String>, _>("coverage_reason_code")
                .as_deref(),
            Some(*expected_reason)
        );
        assert_eq!(event.get::<String, _>("occurred_at"), *occurred_at);
        assert_eq!(event.get::<String, _>("created_at"), *occurred_at);

        let metadata = event.get::<String, _>("legacy_counter_values_json");
        assert_eq!(
            sqlx::query_scalar::<_, Option<String>>("SELECT json_extract(?, '$.source_table')",)
                .bind(&metadata)
                .fetch_one(&pool)
                .await
                .expect("inquiry telemetry source metadata")
                .as_deref(),
            Some("agent_inquiry")
        );
        let metadata_input_type: Option<String> =
            sqlx::query_scalar("SELECT json_extract(?, '$.input_tokens.sqlite_type')")
                .bind(&metadata)
                .fetch_one(&pool)
                .await
                .expect("inquiry counter type metadata");
        if *source_id == "inquiry-positive" || *source_id == "inquiry-zero" {
            assert_eq!(metadata_input_type.as_deref(), Some("integer"));
            let metadata_input: Option<i64> =
                sqlx::query_scalar("SELECT json_extract(?, '$.input_tokens.value')")
                    .bind(&metadata)
                    .fetch_one(&pool)
                    .await
                    .expect("inquiry counter value metadata");
            let expected_input = if *source_id == "inquiry-positive" {
                Some(11)
            } else {
                Some(0)
            };
            assert_eq!(metadata_input, expected_input);
        } else {
            assert_eq!(metadata_input_type.as_deref(), Some("real"));
            assert_eq!(
                sqlx::query_scalar::<_, Option<i64>>(
                    "SELECT json_extract(?, '$.input_tokens.valid')",
                )
                .bind(&metadata)
                .fetch_one(&pool)
                .await
                .expect("malformed inquiry validity metadata"),
                Some(0)
            );
        }
    }

    let ownerless_inquiry_delete = sqlx::query(
        "DELETE FROM usage_event
         WHERE id = 'legacy-inquiry-event:inquiry-malformed'",
    )
    .execute(&pool)
    .await;
    assert!(
        ownerless_inquiry_delete.is_err(),
        "ownerless legacy inquiry events cannot be deleted directly"
    );
    let ownerless_inquiry_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM usage_event
         WHERE id = 'legacy-inquiry-event:inquiry-malformed'",
    )
    .fetch_one(&pool)
    .await
    .expect("ownerless inquiry event remains queryable");
    assert_eq!(ownerless_inquiry_count, 1);

    let imported_legacy_sources: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT legacy_source_table FROM usage_event
         WHERE provenance_kind IN ('legacy_chat', 'legacy_inquiry')
         ORDER BY legacy_source_table",
    )
    .fetch_all(&pool)
    .await
    .expect("legacy source table lookup");
    assert_eq!(
        imported_legacy_sources,
        vec!["agent_chat_message".to_owned(), "agent_inquiry".to_owned()]
    );
    let quarantined_room_import_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM usage_event WHERE legacy_source_id = 'legacy-room-message'",
    )
    .fetch_one(&pool)
    .await
    .expect("quarantined Room import lookup");
    assert_eq!(quarantined_room_import_count, 0);

    // A runtime producer cannot append an event with telemetry that disagrees
    // with the settled chat/inquiry invocation. The owner/scope checks pass in
    // these probes; the invocation guard must reject the deliberately changed
    // telemetry state and leave no partial event behind.
    for (
        probe_id,
        invocation_id,
        surface,
        source_id,
        source_table,
        provider_id,
        model_id,
        agent_id_snapshot,
        profile_id_snapshot,
        executor_type,
        occurred_at,
        model_raw,
        model_sqlite_type,
        model_sql_literal,
    ) in [
        (
            "runtime-chat-event-probe",
            "legacy-chat-invocation:chat-main-valid",
            "main_chat",
            "chat-main-valid",
            "agent_chat_message",
            Some("openai"),
            Some("chat-model"),
            Some(agent_id),
            Some("chat-profile"),
            Some("codex"),
            "2026-09-07T01:00:05Z",
            Some("chat-model"),
            "text",
            "'chat-model'",
        ),
        (
            "runtime-inquiry-event-probe",
            "legacy-inquiry-invocation:inquiry-positive",
            "main_inquiry",
            "inquiry-positive",
            "agent_inquiry",
            Some("openai"),
            Some("chat-model"),
            Some(agent_id),
            Some("chat-profile"),
            Some("codex"),
            "2026-09-07T01:00:20Z",
            None,
            "absent",
            "NULL",
        ),
    ] {
        let provenance_kind = if surface == "main_inquiry" {
            "legacy_inquiry"
        } else {
            "legacy_chat"
        };
        let probe = sqlx::query(
            "INSERT INTO usage_event (
                id, invocation_id, owner_user_id, project_id, surface, source_id,
                event_idempotency_key, source_report_id, report_sequence, report_mode,
                provenance_kind, legacy_source_table, legacy_source_id,
                legacy_provider_raw, legacy_provider_sqlite_type, legacy_provider_sql_literal,
                legacy_model_raw, legacy_model_sqlite_type, legacy_model_sql_literal,
                legacy_counter_values_json, legacy_cost_usd_raw, legacy_created_at_raw,
                legacy_project_owner_raw, provider_id, model_id, runtime_model, candidate_key,
                attempt_ordinal, agent_id, profile_id, executor_type, telemetry_state,
                cost_kind, coverage_reason_code, occurred_at, created_at
             ) VALUES (
                ?, ?, ?, NULL, ?, ?, ?, ?, 0, 'legacy_aggregate', ?, ?, ?,
                NULL, 'absent', 'NULL', ?, ?, ?, '{\"probe\":1}', 'NULL', ?, ?, ?, ?, ?,
                ?, 0, ?, ?, ?, 'unmetered', 'none', 'unmetered', ?, ?
             )",
        )
        .bind(probe_id)
        .bind(invocation_id)
        .bind(owner_id)
        .bind(surface)
        .bind(source_id)
        .bind(format!("{probe_id}-key"))
        .bind(format!("{probe_id}-report"))
        .bind(provenance_kind)
        .bind(source_table)
        .bind(source_id)
        .bind(model_raw)
        .bind(model_sqlite_type)
        .bind(model_sql_literal)
        .bind(format!("'{occurred_at}'"))
        .bind(format!("'{owner_id}'"))
        .bind(provider_id)
        .bind(model_id)
        .bind(model_id)
        .bind(format!("legacy-{surface}-candidate:{source_id}"))
        .bind(agent_id_snapshot)
        .bind(profile_id_snapshot)
        .bind(executor_type)
        .bind(occurred_at)
        .bind(occurred_at)
        .execute(&pool)
        .await;
        assert!(
            probe.is_err(),
            "{surface} runtime telemetry mismatch is rejected"
        );
        let probe_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM usage_event WHERE id = ?")
            .bind(probe_id)
            .fetch_one(&pool)
            .await
            .expect("runtime probe absence lookup");
        assert_eq!(probe_count, 0, "rejected runtime probe leaves no event");
    }

    let before_replay: (i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM pricing_selection WHERE provenance_kind IN ('legacy_chat', 'legacy_inquiry')),
            (SELECT COUNT(*) FROM usage_invocation WHERE provenance_kind IN ('legacy_chat', 'legacy_inquiry')),
            (SELECT COUNT(*) FROM usage_event WHERE provenance_kind IN ('legacy_chat', 'legacy_inquiry'))",
    )
    .fetch_one(&pool)
    .await
    .expect("pre-replay target counts");
    run_migrations(&pool)
        .await
        .expect("V135 replay is idempotent");
    let after_replay: (i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM pricing_selection WHERE provenance_kind IN ('legacy_chat', 'legacy_inquiry')),
            (SELECT COUNT(*) FROM usage_invocation WHERE provenance_kind IN ('legacy_chat', 'legacy_inquiry')),
            (SELECT COUNT(*) FROM usage_event WHERE provenance_kind IN ('legacy_chat', 'legacy_inquiry'))",
    )
    .fetch_one(&pool)
    .await
    .expect("post-replay target counts");
    assert_eq!(after_replay, before_replay);

    let replay_url = format!("sqlite://{}", replay_db_path.display());
    let replay_pool = create_sqlite_pool(&replay_url).await.expect("replay pool");
    run_migrations(&replay_pool)
        .await
        .expect("fresh V134 fixture replay applies");
    let fresh_replay_counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM pricing_selection WHERE provenance_kind IN ('legacy_chat', 'legacy_inquiry')),
            (SELECT COUNT(*) FROM usage_invocation WHERE provenance_kind IN ('legacy_chat', 'legacy_inquiry')),
            (SELECT COUNT(*) FROM usage_event WHERE provenance_kind IN ('legacy_chat', 'legacy_inquiry'))",
    )
    .fetch_one(&replay_pool)
    .await
    .expect("fresh replay target counts");
    assert_eq!(fresh_replay_counts, after_replay);
    replay_pool.close().await;

    let foreign_key_violations: Vec<(String, i64, String, i64)> =
        sqlx::query_as("PRAGMA foreign_key_check")
            .fetch_all(&pool)
            .await
            .expect("foreign key check runs");
    assert!(foreign_key_violations.is_empty());

    pool.close().await;
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(replay_db_path);
    let _ = fs::remove_dir_all(migration_dir);
}
