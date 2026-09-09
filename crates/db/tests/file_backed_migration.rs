use db::{
    create_sqlite_pool, run_migrations, run_migrations_from, AgentActionPolicyResult,
    AgentActionRepo, AgentActionStatus, AgentChatRepo, AgentCommitmentRepo, AgentCommitmentStatus,
    AgentInboxKind, AgentInboxRepo, AgentInboxStatus, AgentLcmRepo, AgentRepo, AgentSessionRepo,
    AgentStatus, CreateAgent, CreateAgentAction, CreateAgentCommitment, CreateAgentInboxItem,
    CreateAgentLcmTimeline, CreateAgentSession, CreateContextManifest, CreateContextManifestSource,
    CreateDomainEvent, CreateForgeMemorySourceBinding, CreateTask, DomainEventRepo, MemoryItem,
    ScopedMemoryRepository, SqliteDb, TaskRepo, User, UserRepo,
};
use sqlx::Row;
use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

fn unique_temp_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("forge-{name}-{}-{nanos}", std::process::id()))
}

async fn migrate_wake_cursor_from_v087(initial_sequence: i64) -> (i64, i64, i64) {
    let migration_dir = unique_temp_path("wake-cutover-migrations");
    fs::create_dir_all(&migration_dir).expect("temp migration dir creates");
    copy_migrations_up_to(87, &migration_dir);

    let db_path = unique_temp_path("wake-cutover-db").with_extension("db");
    let url = format!("sqlite://{}", db_path.display());
    let pool = create_sqlite_pool(&url).await.expect("pool");
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("pre-V088 migrations apply");

    for (id, dedupe_key) in [
        ("wake-cutover-event-1", "wake-cutover-dedupe-1"),
        ("wake-cutover-event-2", "wake-cutover-dedupe-2"),
    ] {
        sqlx::query(
            "INSERT INTO domain_event (
                id, event_type, entity_type, entity_id, actor_type,
                scope_type, scope_id, correlation_id, causation_depth,
                dedupe_key, payload_json, created_at
             ) VALUES (?, 'wake.test', 'project', 'wake-cutover-project',
                       'system', 'project', 'wake-cutover-project', ?, 0, ?, '{}', ?)",
        )
        .bind(id)
        .bind(id)
        .bind(dedupe_key)
        .bind("2026-08-21T00:00:00Z")
        .execute(&pool)
        .await
        .expect("pre-cutover event inserts");
    }
    let cutover_max: i64 = sqlx::query_scalar("SELECT MAX(sequence) FROM domain_event")
        .fetch_one(&pool)
        .await
        .expect("pre-cutover sequence loads");
    sqlx::query(
        "INSERT INTO event_consumer_cursor (
            consumer_name, last_sequence, version, updated_at
         ) VALUES ('agent-wake-turns', ?, 7, '2026-08-21T00:00:00Z')",
    )
    .bind(initial_sequence)
    .execute(&pool)
    .await
    .expect("legacy wake cursor inserts");

    run_migrations(&pool).await.expect("V088 applies");
    let cursor: (i64, i64) = sqlx::query_as(
        "SELECT last_sequence, version
         FROM event_consumer_cursor
         WHERE consumer_name = 'agent-wake-turns'",
    )
    .fetch_one(&pool)
    .await
    .expect("wake cursor loads");
    let cutover: i64 = sqlx::query_scalar(
        "SELECT cutover_sequence
         FROM event_consumer_cutover
         WHERE consumer_name = 'agent-wake-turns'",
    )
    .fetch_one(&pool)
    .await
    .expect("wake cutover loads");

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(migration_dir);
    assert_eq!(cutover, cutover_max);
    (cursor.0, cursor.1, cutover)
}

#[tokio::test]
async fn file_backed_migrations_apply_cleanly() {
    let db_path = unique_temp_path("migtest").with_extension("db");
    let _ = std::fs::remove_file(&db_path);
    let url = format!("sqlite://{}", db_path.display());
    let pool = create_sqlite_pool(&url).await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
}

#[tokio::test]
async fn delivery_followup_backfill_replays_latest_unreconciled_done_task_after_cursor() {
    let migration_dir = unique_temp_path("delivery-followup-migrations");
    fs::create_dir_all(&migration_dir).expect("temp migration dir creates");
    copy_migrations_up_to(92, &migration_dir);

    let db_path = unique_temp_path("delivery-followup-db").with_extension("db");
    let url = format!("sqlite://{}", db_path.display());
    let pool = create_sqlite_pool(&url).await.expect("pool");
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("pre-V093 migrations apply");

    let now = "2026-08-22T20:00:00Z";
    sqlx::query(
        "INSERT INTO project (
            id, name, settings, workflow_definition, created_at, updated_at
         ) VALUES ('followup-project', 'Follow-up', '{}', '{}', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("project inserts");
    sqlx::query(
        "INSERT INTO repo (
            id, project_id, name, remote_url, local_path, work_mode,
            default_branch, created_at, updated_at
         ) VALUES ('followup-repo', 'followup-project', 'repo',
            'https://example.test/followup.git', NULL, 'direct_merge', 'main', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("repository inserts");
    for (task_id, title) in [
        ("followup-task-1", "First delivered task"),
        ("followup-task-2", "Latest delivered task"),
    ] {
        sqlx::query(
            "INSERT INTO task (
                id, project_id, repo_id, title, status, created_at, updated_at
             ) VALUES (?, 'followup-project', 'followup-repo', ?, 'done', ?, ?)",
        )
        .bind(task_id)
        .bind(title)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .expect("done task inserts");
        sqlx::query(
            "INSERT INTO domain_event (
                id, event_type, entity_type, entity_id, actor_type, actor_id,
                scope_type, scope_id, correlation_id, causation_depth,
                dedupe_key, payload_json, created_at
             ) VALUES (?, 'task.transitioned', 'task', ?, 'system', 'workflow',
                'task', ?, ?, 0, ?, '{\"to_state\":\"done\"}', ?)",
        )
        .bind(format!("{task_id}-done-event"))
        .bind(task_id)
        .bind(task_id)
        .bind(format!("{task_id}-correlation"))
        .bind(format!("{task_id}-done-dedupe"))
        .bind(now)
        .execute(&pool)
        .await
        .expect("done transition event inserts");
    }
    let old_cursor: i64 = sqlx::query_scalar("SELECT MAX(sequence) FROM domain_event")
        .fetch_one(&pool)
        .await
        .expect("legacy event sequence loads");
    sqlx::query(
        "INSERT INTO event_consumer_cursor (
            consumer_name, last_sequence, version, updated_at
         ) VALUES ('attention_projection', ?, 1, ?)
         ON CONFLICT(consumer_name) DO UPDATE SET last_sequence = excluded.last_sequence",
    )
    .bind(old_cursor)
    .bind(now)
    .execute(&pool)
    .await
    .expect("attention cursor advances past legacy events");

    run_migrations(&pool)
        .await
        .expect("V093 delivery follow-up backfill applies");
    let synthetic: (String, i64, String) = sqlx::query_as(
        "SELECT entity_id, sequence, dedupe_key
         FROM domain_event
         WHERE event_type = 'task.completed' AND actor_id = 'V093__delivery_followup_backfill'",
    )
    .fetch_one(&pool)
    .await
    .expect("one follow-up event is appended");
    assert_eq!(synthetic.0, "followup-task-2");
    assert!(synthetic.1 > old_cursor);
    assert!(synthetic
        .2
        .starts_with("migration:delivery-followup:v1:followup-project:"));
    let synthetic_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event
         WHERE event_type = 'task.completed' AND actor_id = 'V093__delivery_followup_backfill'",
    )
    .fetch_one(&pool)
    .await
    .expect("follow-up count loads");
    assert_eq!(synthetic_count, 1);

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(migration_dir);
}

#[tokio::test]
async fn delivery_followup_postcondition_recovery_replays_open_incident_once() {
    let migration_dir = unique_temp_path("delivery-followup-postcondition-migrations");
    fs::create_dir_all(&migration_dir).expect("temp migration dir creates");
    copy_migrations_up_to(93, &migration_dir);

    let db_path = unique_temp_path("delivery-followup-postcondition-db").with_extension("db");
    let url = format!("sqlite://{}", db_path.display());
    let pool = create_sqlite_pool(&url).await.expect("pool");
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("pre-V094 migrations apply");

    let now = "2026-08-23T04:00:00Z";
    sqlx::query(
        "INSERT INTO project (
            id, name, settings, workflow_definition, created_at, updated_at
         ) VALUES ('postcondition-project', 'Postcondition', '{}', '{}', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("project inserts");
    sqlx::query(
        "INSERT INTO task (
            id, project_id, title, status, created_at, updated_at
         ) VALUES ('postcondition-task', 'postcondition-project',
                   'Delivered task', 'done', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("done task inserts");
    sqlx::query(
        "INSERT INTO domain_event (
            id, event_type, entity_type, entity_id, actor_type, actor_id,
            scope_type, scope_id, correlation_id, causation_depth,
            dedupe_key, payload_json, created_at
         ) VALUES ('postcondition-done-event', 'task.transitioned', 'task',
                   'postcondition-task', 'system', 'workflow', 'task',
                   'postcondition-task', 'postcondition-correlation', 0,
                   'postcondition-done-dedupe', '{\"to_state\":\"done\"}', ?)",
    )
    .bind(now)
    .execute(&pool)
    .await
    .expect("done transition event inserts");
    let source_sequence: i64 = sqlx::query_scalar(
        "SELECT sequence FROM domain_event WHERE id = 'postcondition-done-event'",
    )
    .fetch_one(&pool)
    .await
    .expect("done event sequence loads");
    sqlx::query(
        "INSERT INTO attention_projection (
            id, attention_type, scope_type, scope_id, source_event_id,
            priority, status, summary, details_json, dedupe_key, occurred_at,
            updated_at, recommended_action, source_sequence
         ) VALUES ('postcondition-attention', 'delivery_followup', 'project',
                   'postcondition-project', 'postcondition-done-event', 70,
                   'open', 'Reconcile delivery',
                   '{\"scope_type\":\"project\",\"scope_id\":\"postcondition-project\"}',
                   'postcondition-incident', ?, ?, 'reconcile_delivery', ?)",
    )
    .bind(now)
    .bind(now)
    .bind(source_sequence)
    .execute(&pool)
    .await
    .expect("open delivery Attention inserts");

    run_migrations(&pool)
        .await
        .expect("V094 postcondition recovery applies");
    let recovered: (String, String, i64) = sqlx::query_as(
        "SELECT entity_id, dedupe_key, sequence
         FROM domain_event
         WHERE event_type = 'task.completed'
           AND actor_id = 'V094__delivery_followup_postcondition_recovery'",
    )
    .fetch_one(&pool)
    .await
    .expect("one recovery event is appended");
    assert_eq!(recovered.0, "postcondition-task");
    assert!(recovered
        .1
        .starts_with("migration:delivery-followup-postcondition:v1:postcondition-project:"));
    assert!(recovered.2 > source_sequence);

    run_migrations(&pool)
        .await
        .expect("V094 replay remains idempotent");
    let recovered_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event
         WHERE event_type = 'task.completed'
           AND actor_id = 'V094__delivery_followup_postcondition_recovery'",
    )
    .fetch_one(&pool)
    .await
    .expect("recovery count loads");
    assert_eq!(recovered_count, 1);

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(migration_dir);
}

#[tokio::test]
async fn wake_cutover_advances_existing_lagging_cursor_but_preserves_ahead_cursor() {
    let (lagging_sequence, lagging_version, cutover) = migrate_wake_cursor_from_v087(0).await;
    assert_eq!(lagging_sequence, cutover);
    assert_eq!(lagging_version, 8);

    let (ahead_sequence, ahead_version, ahead_cutover) =
        migrate_wake_cursor_from_v087(cutover + 10).await;
    assert_eq!(ahead_cutover, cutover);
    assert_eq!(ahead_sequence, cutover + 10);
    assert_eq!(ahead_version, 7);
}

#[tokio::test]
async fn cursor_executor_backfill_runs_when_version_53_was_used_by_old_migration() {
    let migration_dir = unique_temp_path("cursor-backfill-migrations");
    fs::create_dir_all(&migration_dir).expect("temp migration dir creates");
    copy_migrations_up_to(52, &migration_dir);

    let db_path = unique_temp_path("cursor-backfill-db").with_extension("db");
    let url = format!("sqlite://{}", db_path.display());
    let pool = create_sqlite_pool(&url).await.expect("pool");

    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("baseline migrations apply");

    sqlx::query(
        "INSERT INTO _migration (version, name, applied_at) VALUES (53, 'integration_credentials', '2026-05-25T00:00:00Z')",
    )
    .execute(&pool)
    .await
    .expect("old conflicting migration marker inserts");

    run_migrations(&pool)
        .await
        .expect("current migrations backfill cursor executor type");

    let migration_name: String =
        sqlx::query_scalar("SELECT name FROM _migration WHERE version = 54")
            .fetch_one(&pool)
            .await
            .expect("V054 migration applied");
    assert_eq!(migration_name, "cursor_executor_type_backfill");

    AgentRepo::create(
        &SqliteDb::new(pool.clone()),
        CreateAgent {
            id: "cursor-agent".to_owned(),
            name: "Cursor".to_owned(),
            description: None,
            executor_type: "cursor".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: "now".to_owned(),
            updated_at: "now".to_owned(),
        },
    )
    .await
    .expect("cursor executor type is accepted");

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(migration_dir);
}

#[tokio::test]
async fn agent_identity_profile_membership_migration_preserves_operational_history() {
    let migration_dir = unique_temp_path("agent-identity-baseline-migrations");
    fs::create_dir_all(&migration_dir).expect("temp migration dir creates");
    copy_migrations_up_to(58, &migration_dir);

    let db_path = unique_temp_path("agent-identity-baseline-db").with_extension("db");
    let url = format!("sqlite://{}", db_path.display());
    let pool = create_sqlite_pool(&url).await.expect("pool");
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("baseline migrations apply");

    let now = "2026-08-12T00:00:00Z";
    sqlx::query(
        "INSERT INTO project (id, name, settings, workflow_definition, created_at, updated_at) VALUES ('project-1', 'Forge', '{}', '{}', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("project inserts");
    sqlx::query(
        "INSERT INTO agent (id, name, description, executor_type, model, reasoning_effort, permission_policy, prompt_template, capabilities_json, config_json, max_concurrent_tasks, status, is_default, paused, owner_id, visibility, created_at, updated_at) VALUES ('agent-1', 'Steward', 'durable teammate', 'codex', 'gpt-5.6', 'high', 'read-only', 'keep scope', '[\"rust\"]', '{\"sandbox\":\"read-only\"}', 2, 'idle', 1, 0, 'user-1', 'account', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("legacy agent inserts");
    sqlx::query(
        "INSERT INTO project_agent_link (id, project_id, agent_id, linked_by_user_id, created_at, updated_at) VALUES ('link-1', 'project-1', 'agent-1', 'user-1', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("legacy project link inserts");
    sqlx::query(
        "INSERT INTO task (id, project_id, title, assignee_type, assignee_id, created_at, updated_at) VALUES ('task-1', 'project-1', 'Preserve me', 'agent', 'agent-1', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("task inserts");
    sqlx::query(
        "INSERT INTO task_role_assignment (id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at) VALUES ('role-1', 'task-1', 'worker', 'agent', 'agent-1', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("role assignment inserts");
    sqlx::query(
        "INSERT INTO execution (id, task_id, agent_id, role, status, agent_session_id, summary, created_at, updated_at) VALUES ('execution-1', 'task-1', 'agent-1', 'executor', 'completed', 'task-session-1', 'delivered', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("execution inserts");
    sqlx::query(
        "INSERT INTO conversation (id, project_id, agent_id, title, system_prompt, message_count, last_message_at, agent_session_id, created_at, updated_at) VALUES ('conversation-1', 'project-1', 'agent-1', 'History', 'stay read only', 2, ?, 'room-session-1', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("conversation inserts");
    sqlx::query(
        "INSERT INTO conversation_message (id, conversation_id, role, content, status, sequence, created_at, updated_at) VALUES ('message-1', 'conversation-1', 'user', 'remember this', 'complete', 1, ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("message inserts");
    sqlx::query(
        "INSERT INTO conversation_message (id, conversation_id, role, content, status, model, token_usage_json, duration_ms, error, sequence, created_at, updated_at) VALUES ('message-2', 'conversation-1', 'assistant', 'partial answer', 'streaming', 'gpt-5.6', '{\"input_tokens\":12}', 45, NULL, 2, ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("streaming assistant message inserts");
    sqlx::query(
        "INSERT INTO memory_item (id, project_id, task_id, execution_id, conversation_id, source_type, kind, title, body, created_at) VALUES ('memory-1', 'project-1', 'task-1', 'execution-1', 'conversation-1', 'conversation_message', 'observation', 'Remembered', 'remember this', ?)",
    )
    .bind(now)
    .execute(&pool)
    .await
    .expect("memory inserts");
    sqlx::query(
        "INSERT INTO memory_item (id, project_id, conversation_id, source_type, kind, title, body, created_at) VALUES ('memory-secret', 'project-1', 'conversation-1', 'conversation_message', 'observation', 'Secret legacy note', 'Authorization: Bearer sk-test-secret', ?)",
    )
    .bind(now)
    .execute(&pool)
    .await
    .expect("secret memory inserts");

    run_migrations(&pool)
        .await
        .expect("identity/profile migration applies");

    let legacy_agent_table_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'agent'",
    )
    .fetch_one(&pool)
    .await
    .expect("legacy table check runs");
    assert_eq!(legacy_agent_table_count, 0);
    for table in [
        "room",
        "room_message",
        "agent_turn_job",
        "project_agent_membership",
    ] {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(table)
        .fetch_one(&pool)
        .await
        .expect("retired table check runs");
        assert_eq!(count, 0, "legacy runtime table remains live: {table}");
    }

    let identity: (String, String, Option<String>) = sqlx::query_as(
        "SELECT id, name, selected_profile_id FROM agent_identity WHERE id = 'agent-1'",
    )
    .fetch_one(&pool)
    .await
    .expect("identity loads");
    assert_eq!(identity.0, "agent-1");
    assert_eq!(identity.1, "Steward");
    let profile_id = identity.2.expect("selected profile preserved");

    let profile: (String, String, Option<String>, String) = sqlx::query_as(
        "SELECT backend_kind, executor_type, model, capabilities_json FROM agent_profile WHERE id = ? AND identity_id = 'agent-1'",
    )
    .bind(&profile_id)
    .fetch_one(&pool)
    .await
    .expect("profile loads");
    assert_eq!(profile.0, "cli");
    assert_eq!(profile.1, "codex");
    assert_eq!(profile.2.as_deref(), Some("gpt-5.6"));
    assert_eq!(profile.3, "[\"rust\"]");

    let membership: (String, String, String, i64) = sqlx::query_as(
        "SELECT id, identity_id, state, version FROM legacy_project_agent_membership WHERE project_id = 'project-1'",
    )
    .fetch_one(&pool)
    .await
    .expect("membership loads");
    assert_eq!(
        membership,
        (
            "link-1".to_owned(),
            "agent-1".to_owned(),
            "active".to_owned(),
            1
        )
    );

    let task_assignee: Option<String> =
        sqlx::query_scalar("SELECT assignee_id FROM task WHERE id = 'task-1'")
            .fetch_one(&pool)
            .await
            .expect("task assignee loads");
    let execution_agent: Option<String> =
        sqlx::query_scalar("SELECT agent_id FROM execution WHERE id = 'execution-1'")
            .fetch_one(&pool)
            .await
            .expect("execution agent loads");
    let execution_session: Option<String> =
        sqlx::query_scalar("SELECT agent_session_id FROM execution WHERE id = 'execution-1'")
            .fetch_one(&pool)
            .await
            .expect("execution session loads");
    let room_responder: Option<String> = sqlx::query_scalar(
        "SELECT default_responder_identity_id FROM legacy_room WHERE id = 'conversation-1'",
    )
    .fetch_one(&pool)
    .await
    .expect("Room responder loads");
    let message_body: String =
        sqlx::query_scalar("SELECT content FROM legacy_room_message WHERE id = 'message-1'")
            .fetch_one(&pool)
            .await
            .expect("message loads");
    let interrupted_message: (String, String, Option<String>, Option<String>, Option<i64>, Option<String>) =
        sqlx::query_as(
            "SELECT status, outcome, model, token_usage_json, duration_ms, error FROM legacy_room_message WHERE id = 'message-2'",
        )
        .fetch_one(&pool)
        .await
        .expect("interrupted message loads");
    let protected_room_session: String = sqlx::query_scalar(
        "SELECT opaque_session_ref FROM protected_legacy_session_ref WHERE room_id = 'conversation-1'",
    )
    .fetch_one(&pool)
    .await
    .expect("protected legacy Room session loads");
    let memory: (String, Option<String>) =
        sqlx::query_as("SELECT body, room_id FROM memory_item WHERE id = 'memory-1'")
            .fetch_one(&pool)
            .await
            .expect("memory loads");
    let secret_memory: (String, String) =
        sqlx::query_as("SELECT body, sensitivity FROM memory_item WHERE id = 'memory-secret'")
            .fetch_one(&pool)
            .await
            .expect("secret memory loads");
    let secret_audit_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM content_guard_audit WHERE entity_type = 'memory_item' AND entity_id = 'memory-secret'",
    )
    .fetch_one(&pool)
    .await
    .expect("secret memory audit loads");
    let migrated_room_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE entity_type = 'room_message' AND entity_id = 'message-1'",
    )
    .fetch_one(&pool)
    .await
    .expect("Room events load");
    assert_eq!(task_assignee.as_deref(), Some("agent-1"));
    assert_eq!(execution_agent.as_deref(), Some("agent-1"));
    assert_eq!(execution_session.as_deref(), Some("task-session-1"));
    assert_eq!(room_responder.as_deref(), Some("agent-1"));
    assert_eq!(message_body, "remember this");
    assert_eq!(interrupted_message.0, "failed");
    assert_eq!(interrupted_message.1, "interrupted_migration");
    assert_eq!(interrupted_message.2.as_deref(), Some("gpt-5.6"));
    assert_eq!(
        interrupted_message.3.as_deref(),
        Some("{\"input_tokens\":12}")
    );
    assert_eq!(interrupted_message.4, Some(45));
    assert_eq!(
        interrupted_message.5.as_deref(),
        Some("interrupted during scoped Room migration")
    );
    assert_eq!(protected_room_session, "room-session-1");
    assert_eq!(memory.0, "remember this");
    assert_eq!(memory.1.as_deref(), Some("conversation-1"));
    assert_eq!(
        secret_memory.0,
        "[protected value redacted during migration]"
    );
    assert_eq!(secret_memory.1, "restricted");
    assert_eq!(secret_audit_count, 1);
    assert_eq!(migrated_room_events, 1);

    let immutable_update = sqlx::query("UPDATE agent_profile SET model = 'changed' WHERE id = ?")
        .bind(&profile_id)
        .execute(&pool)
        .await;
    assert!(immutable_update.is_err());

    let foreign_key_violations: Vec<(String, i64, String, i64)> =
        sqlx::query_as("PRAGMA foreign_key_check")
            .fetch_all(&pool)
            .await
            .expect("foreign key check runs");
    assert!(foreign_key_violations.is_empty());

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(migration_dir);
}

#[tokio::test]
async fn discovery_task_type_migration_preserves_rows_and_constraints() {
    let migration_dir = unique_temp_path("discovery-task-migrations");
    fs::create_dir_all(&migration_dir).expect("temp migration dir creates");
    copy_migrations_up_to(72, &migration_dir);

    let db_path = unique_temp_path("discovery-task-db").with_extension("db");
    let url = format!("sqlite://{}", db_path.display());
    let pool = create_sqlite_pool(&url).await.expect("pool");
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("pre-discovery migrations apply");

    let now = "2026-08-13T00:00:00Z";
    sqlx::query(
        "INSERT INTO project (id, name, settings, workflow_definition, created_at, updated_at) VALUES ('discovery-project', 'Discovery', '{}', '{}', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("project inserts");
    sqlx::query(
        "INSERT INTO repo (id, project_id, name, remote_url, local_path, work_mode, default_branch, created_at, updated_at) VALUES ('discovery-repo', 'discovery-project', 'repo', 'https://example.test/discovery.git', NULL, 'direct_merge', 'main', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("repo inserts");
    sqlx::query(
        "INSERT INTO task (id, project_id, repo_id, title, created_at, updated_at) VALUES ('legacy-task', 'discovery-project', 'discovery-repo', 'Legacy task', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("legacy task inserts");

    run_migrations(&pool)
        .await
        .expect("discovery task migration applies");
    let db = SqliteDb::new(pool.clone());

    let legacy = TaskRepo::get_by_id(&db, "legacy-task", true)
        .await
        .expect("legacy task loads")
        .expect("legacy task survives");
    assert_eq!(legacy.title, "Legacy task");
    assert_eq!(legacy.task_type, "task");

    let discovery = TaskRepo::create(
        &db,
        CreateTask {
            id: "discovery-task".to_owned(),
            project_id: "discovery-project".to_owned(),
            repo_id: Some("discovery-repo".to_owned()),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "Discover product direction".to_owned(),
            description: Some("Genesis discovery".to_owned()),
            task_type: "discovery".to_owned(),
            status: "todo".to_owned(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("discovery task creates through repository");
    assert_eq!(discovery.task_type, "discovery");

    let task_sql: String =
        sqlx::query_scalar("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'task'")
            .fetch_one(&pool)
            .await
            .expect("task schema loads");
    assert!(task_sql.contains("'discovery'"));
    for object in [
        "idx_task_status_project",
        "idx_task_parent",
        "idx_task_repo",
        "idx_task_assignee",
        "idx_task_parent_subtask_order",
        "idx_task_project_archived",
        "idx_task_project_automation",
        "task_insert_requires_assignee_id",
        "task_board_revision_after_insert",
        "task_board_revision_after_delete",
        "task_board_revision_after_update",
    ] {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE name = ?")
            .bind(object)
            .fetch_one(&pool)
            .await
            .expect("schema object lookup");
        assert_eq!(count, 1, "schema object {object} should survive rebuild");
    }
    let foreign_key_violations: Vec<(String, i64, String, i64)> =
        sqlx::query_as("PRAGMA foreign_key_check")
            .fetch_all(&pool)
            .await
            .expect("foreign key check runs");
    assert!(foreign_key_violations.is_empty());

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(migration_dir);
}

#[tokio::test]
async fn agent_chat_scope_rebuild_preserves_legacy_rows_and_relationships() {
    let migration_dir = unique_temp_path("agent-chat-scope-migrations");
    fs::create_dir_all(&migration_dir).expect("temp migration dir creates");
    copy_migrations_up_to(73, &migration_dir);

    let db_path = unique_temp_path("agent-chat-scope-db").with_extension("db");
    let url = format!("sqlite://{}", db_path.display());
    let pool = create_sqlite_pool(&url).await.expect("pool");
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("pre-Agent Chat migrations apply");
    let db = SqliteDb::new(pool.clone());
    let now = "2026-08-13T00:00:00Z";
    let account_id = "scope-rebuild-account";
    let identity_id = "scope-rebuild-identity";

    UserRepo::create_user(
        &db,
        &User {
            id: account_id.to_owned(),
            email: "scope-rebuild@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("account creates");
    let identity = AgentRepo::create(
        &db,
        CreateAgent {
            id: identity_id.to_owned(),
            name: "Scope Rebuild Agent".to_owned(),
            description: None,
            executor_type: "null".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some(account_id.to_owned()),
            visibility: "account".to_owned(),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("identity creates");
    let chat = AgentChatRepo::get_main_chat(&db, account_id)
        .await
        .expect("main chat lookup")
        .expect("main chat exists");
    assert_eq!(chat.account_id.as_deref(), Some(account_id));
    // The repository writes today's column set; this schema is frozen at
    // migration 73, before `workspace_path` existed. Insert the legacy row
    // shape directly so the rebuild is exercised on genuinely old data.
    sqlx::query(
        "INSERT INTO agent_context_scope (
            id, identity_id, scope_type, scope_id, project_id, task_id,
            task_role, workspace_access, authority_json, created_at, updated_at
         ) VALUES (?, ?, 'account', ?, NULL, NULL, NULL, 'deny', '{}', ?, ?)",
    )
    .bind("scope-rebuild-context")
    .bind(&identity.id)
    .bind(account_id)
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("legacy context scope creates");
    let scope = db::AgentContextScope {
        id: "scope-rebuild-context".to_owned(),
        identity_id: identity.id.clone(),
        scope_type: "account".to_owned(),
        scope_id: account_id.to_owned(),
        project_id: None,
        task_id: None,
        task_role: None,
        workspace_access: "deny".to_owned(),
        workspace_path: None,
        authority_json: "{}".to_owned(),
        version: 1,
        created_at: now.to_owned(),
        updated_at: now.to_owned(),
    };
    let session = AgentSessionRepo::create_agent_session(
        &db,
        CreateAgentSession {
            id: "scope-rebuild-session".to_owned(),
            identity_id: identity.id.clone(),
            profile_id: identity.profile_id.clone(),
            context_scope_id: scope.id.clone(),
            backend_kind: "cli".to_owned(),
            runtime_session_id: None,
            status: "ready".to_owned(),
            capabilities_json: "{}".to_owned(),
            connection_status: "unknown".to_owned(),
            predecessor_session_id: None,
            last_activity_at: None,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("legacy session creates");
    let timeline = AgentLcmRepo::create_or_get_lcm_timeline(
        &db,
        CreateAgentLcmTimeline {
            id: "scope-rebuild-lcm".to_owned(),
            identity_id: identity.id.clone(),
            scope_type: "account".to_owned(),
            scope_id: account_id.to_owned(),
            authorization_revision: "auth-legacy".to_owned(),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("legacy LCM timeline creates");
    let memory = MemoryItem {
        row_id: 0,
        id: "scope-rebuild-memory".to_owned(),
        project_id: None,
        task_id: None,
        execution_id: None,
        scope_type: "account".to_owned(),
        scope_id: account_id.to_owned(),
        visibility: "account".to_owned(),
        owner_identity_id: Some(identity.id.clone()),
        authority: "observation".to_owned(),
        sensitivity: "internal".to_owned(),
        retention_priority: 1,
        provenance_json: "{}".to_owned(),
        publication_source_id: None,
        supersedes_id: None,
        valid_from: None,
        valid_until: None,
        source_event_id: None,
        source_scope_type: Some("account".to_owned()),
        source_scope_id: Some(account_id.to_owned()),
        source_revision: Some("legacy".to_owned()),
        source_type: "native".to_owned(),
        kind: "observation".to_owned(),
        title: "Legacy scope memory".to_owned(),
        summary: None,
        body: "Preserve this body".to_owned(),
        metadata_json: "{\"source_ref\":\"scope-rebuild-memory-source\"}".to_owned(),
        confidence: None,
        quality_score: None,
        created_by_type: Some("agent".to_owned()),
        created_by_id: Some(identity.id.clone()),
        created_at: now.to_owned(),
    };
    db::MemoryRepository::insert_memory_item(&db, &memory)
        .await
        .expect("legacy memory creates");
    sqlx::query(
        "INSERT INTO memory_source_receipt (
            source_type, source_scope_type, source_scope_id, source_ref,
            memory_item_id, created_at
         ) VALUES ('native', 'account', ?, 'scope-rebuild-memory-source', ?, ?)",
    )
    .bind(account_id)
    .bind(&memory.id)
    .bind(now)
    .execute(&pool)
    .await
    .expect("legacy memory source receipt creates");
    ScopedMemoryRepository::create_memory_source_binding(
        &db,
        CreateForgeMemorySourceBinding {
            id: "scope-rebuild-binding".to_owned(),
            identity_id: identity.id.clone(),
            context_scope_id: scope.id.clone(),
            scope_type: "account".to_owned(),
            scope_id: account_id.to_owned(),
            account_id: Some(account_id.to_owned()),
            project_id: None,
            task_id: None,
            policy_revision: "legacy-policy".to_owned(),
            created_at: now.to_owned(),
        },
    )
    .await
    .expect("legacy memory binding creates");
    let manifest = ScopedMemoryRepository::create_context_manifest(
        &db,
        CreateContextManifest {
            id: "scope-rebuild-manifest".to_owned(),
            identity_id: identity.id.clone(),
            agent_session_id: Some(session.id.clone()),
            context_scope_id: scope.id.clone(),
            scope_type: "account".to_owned(),
            scope_id: account_id.to_owned(),
            policy_revision: "legacy-policy".to_owned(),
            domain_revision: "legacy-domain".to_owned(),
            lcm_binding_revision: Some(timeline.revision.to_string()),
            runtime_manifest_id: None,
            runtime_manifest_fingerprint: None,
            combined_fingerprint: "legacy-combined".to_owned(),
            request_fingerprint: "legacy-request".to_owned(),
            created_at: now.to_owned(),
        },
    )
    .await
    .expect("legacy manifest creates");
    ScopedMemoryRepository::append_context_manifest_source(
        &db,
        CreateContextManifestSource {
            manifest_id: manifest.id,
            ordinal: 0,
            source_id: memory.id.clone(),
            source_type: "memory_item".to_owned(),
            source_revision: "legacy".to_owned(),
            selection_reason: "legacy source".to_owned(),
            disposition: "included".to_owned(),
            retention_priority: 1,
            fragment_fingerprint: "legacy-fragment".to_owned(),
        },
    )
    .await
    .expect("legacy manifest source creates");
    AgentActionRepo::create_action(
        &db,
        CreateAgentAction {
            id: "scope-rebuild-action".to_owned(),
            actor_identity_id: identity.id.clone(),
            scope_type: "account".to_owned(),
            scope_id: account_id.to_owned(),
            operation: "legacy.observe".to_owned(),
            payload_json: "{}".to_owned(),
            payload_hash: "legacy-action-hash".to_owned(),
            dedupe_key: "legacy-action-dedupe".to_owned(),
            correlation_id: "legacy-correlation".to_owned(),
            causation_id: None,
            causation_depth: 0,
            requested_permission: "account.read".to_owned(),
            policy_result: AgentActionPolicyResult::Allowed,
            policy_reason: None,
            status: AgentActionStatus::Proposed,
            target_type: None,
            target_id: None,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("legacy action creates");
    AgentCommitmentRepo::create_commitment(
        &db,
        CreateAgentCommitment {
            id: "scope-rebuild-commitment".to_owned(),
            owner_identity_id: identity.id.clone(),
            scope_type: "account".to_owned(),
            scope_id: account_id.to_owned(),
            title: "Legacy commitment".to_owned(),
            description: None,
            status: AgentCommitmentStatus::Open,
            due_at: None,
            correlation_id: "legacy-correlation".to_owned(),
            originating_action_id: None,
            originating_task_id: None,
            evidence_required: false,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("legacy commitment creates");
    AgentInboxRepo::create_inbox_item(
        &db,
        CreateAgentInboxItem {
            id: "scope-rebuild-inbox".to_owned(),
            recipient_identity_id: identity.id.clone(),
            scope_type: "account".to_owned(),
            scope_id: account_id.to_owned(),
            kind: AgentInboxKind::Message,
            status: AgentInboxStatus::Unread,
            title: "Legacy inbox".to_owned(),
            body: "Legacy inbox body".to_owned(),
            payload_json: "{}".to_owned(),
            source_type: None,
            source_id: None,
            correlation_id: "legacy-correlation".to_owned(),
            causation_id: None,
            dedupe_key: "legacy-inbox-dedupe".to_owned(),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("legacy inbox creates");
    AgentInboxRepo::create_question_with_inbox(
        &db,
        CreateAgentInboxItem {
            id: "scope-rebuild-question-inbox".to_owned(),
            recipient_identity_id: identity.id.clone(),
            scope_type: "account".to_owned(),
            scope_id: account_id.to_owned(),
            kind: AgentInboxKind::Question,
            status: AgentInboxStatus::Unread,
            title: "Legacy question".to_owned(),
            body: "Legacy question body".to_owned(),
            payload_json: "{}".to_owned(),
            source_type: None,
            source_id: None,
            correlation_id: "legacy-correlation".to_owned(),
            causation_id: None,
            dedupe_key: "legacy-question-dedupe".to_owned(),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
        db::CreateAgentQuestion {
            id: "scope-rebuild-question".to_owned(),
            recipient_identity_id: identity.id.clone(),
            scope_type: "account".to_owned(),
            scope_id: account_id.to_owned(),
            question: "Legacy question?".to_owned(),
            context_json: "{}".to_owned(),
            asked_by_type: "agent".to_owned(),
            asked_by_id: identity.id.clone(),
            inbox_item_id: Some("scope-rebuild-question-inbox".to_owned()),
            due_at: None,
            correlation_id: "legacy-correlation".to_owned(),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("legacy question creates");
    sqlx::query(
        "INSERT INTO agent_wake_lease (
            identity_id, scope_type, scope_id, incident_key, lease_owner,
            leased_until, reaction_depth, updated_at
         ) VALUES (?, 'account', ?, 'legacy-incident', 'legacy-worker', ?, 0, ?)",
    )
    .bind(&identity.id)
    .bind(account_id)
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("legacy wake lease creates");
    sqlx::query(
        "INSERT INTO agent_wake_budget_window (
            identity_id, scope_type, scope_id, window_started_at, updated_at
         ) VALUES (?, 'account', ?, ?, ?)",
    )
    .bind(&identity.id)
    .bind(account_id)
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("legacy wake budget creates");
    DomainEventRepo::append_event(
        &db,
        CreateDomainEvent {
            id: "scope-rebuild-event".to_owned(),
            event_type: "legacy.observed".to_owned(),
            entity_type: "agent".to_owned(),
            entity_id: identity.id.clone(),
            actor_type: "agent".to_owned(),
            actor_id: Some(identity.id.clone()),
            scope_type: "account".to_owned(),
            scope_id: account_id.to_owned(),
            correlation_id: "legacy-correlation".to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("scope-rebuild-event-dedupe".to_owned()),
            payload_json: "{}".to_owned(),
            created_at: now.to_owned(),
        },
    )
    .await
    .expect("legacy event creates");

    run_migrations(&pool)
        .await
        .expect("Agent Chat scope migration applies");

    for (table, id_column, id) in [
        ("agent_context_scope", "id", "scope-rebuild-context"),
        ("agent_session", "id", "scope-rebuild-session"),
        ("agent_lcm_timeline", "id", "scope-rebuild-lcm"),
        ("memory_item", "id", "scope-rebuild-memory"),
        ("forge_memory_source_binding", "id", "scope-rebuild-binding"),
        ("agent_action", "id", "scope-rebuild-action"),
        ("agent_commitment", "id", "scope-rebuild-commitment"),
        ("agent_inbox_item", "id", "scope-rebuild-inbox"),
        ("agent_question", "id", "scope-rebuild-question"),
        ("domain_event", "id", "scope-rebuild-event"),
    ] {
        let sql = format!("SELECT COUNT(*) FROM {table} WHERE {id_column} = ?");
        let count: i64 = sqlx::query_scalar(&sql)
            .bind(id)
            .fetch_one(&pool)
            .await
            .expect("preserved row lookup");
        assert_eq!(count, 1, "{table}.{id_column} {id} should survive");
    }
    let body: String =
        sqlx::query_scalar("SELECT body FROM memory_item WHERE id = 'scope-rebuild-memory'")
            .fetch_one(&pool)
            .await
            .expect("preserved memory body");
    assert_eq!(body, "Preserve this body");
    let receipt_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM memory_source_receipt WHERE memory_item_id = 'scope-rebuild-memory'",
    )
    .fetch_one(&pool)
    .await
    .expect("preserved memory source receipt lookup");
    assert_eq!(receipt_count, 1);
    let fts_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM memory_item_fts WHERE memory_item_fts MATCH 'Preserve'",
    )
    .fetch_one(&pool)
    .await
    .expect("preserved memory FTS lookup");
    assert_eq!(fts_count, 1);

    for object in [
        "idx_domain_event_dedupe",
        "idx_domain_event_scope_sequence",
        "idx_domain_event_entity_sequence",
        "idx_domain_event_type_sequence",
        "idx_agent_context_scope_scope",
        "agent_context_scope_identity_profile_guard",
        "agent_context_scope_identity_profile_guard_update",
        "idx_agent_lcm_timeline_scope",
        "idx_memory_item_project",
        "idx_memory_item_task",
        "idx_memory_item_room",
        "idx_memory_item_scope",
        "idx_memory_item_owner",
        "idx_memory_item_authority",
        "idx_memory_item_source_scope",
        "idx_memory_item_created_at",
        "memory_item_ai",
        "memory_item_ad",
        "memory_item_immutable_update",
        "forge_memory_source_binding_immutable_update",
        "forge_memory_source_binding_immutable_delete",
        "context_manifest_immutable_update",
        "context_manifest_immutable_delete",
        "context_manifest_source_immutable_update",
        "context_manifest_source_immutable_delete",
        "idx_agent_commitment_owner_status",
        "idx_agent_commitment_scope_status",
        "idx_agent_commitment_originating_task",
        "idx_agent_inbox_recipient_status",
        "idx_agent_inbox_scope",
        "idx_agent_question_recipient_status",
        "idx_agent_question_scope_status",
        "idx_agent_question_inbox_item",
        "idx_agent_action_scope_status",
        "idx_agent_action_actor_status",
        "idx_agent_wake_budget_window_updated",
    ] {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE name = ?")
            .bind(object)
            .fetch_one(&pool)
            .await
            .expect("recreated schema object lookup");
        assert_eq!(
            count, 1,
            "schema object {object} should survive scope rebuild"
        );
    }

    let foreign_key_violations: Vec<(String, i64, String, i64)> =
        sqlx::query_as("PRAGMA foreign_key_check")
            .fetch_all(&pool)
            .await
            .expect("foreign key check runs");
    assert!(foreign_key_violations.is_empty());

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(migration_dir);
}

fn copy_migrations_up_to(max_version: i64, destination: &Path) {
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
        if version <= max_version {
            fs::copy(&path, destination.join(filename)).expect("migration copies");
        }
    }
}

fn migration_version(filename: &str) -> Option<i64> {
    filename.strip_prefix('V')?.split_once("__")?.0.parse().ok()
}

#[tokio::test]
async fn singular_agent_chat_migration_merges_threads_and_recovers_turns() {
    let migration_dir = unique_temp_path("singular-chat-matrix-migrations");
    fs::create_dir_all(&migration_dir).expect("temp migration dir creates");
    copy_migrations_up_to(58, &migration_dir);

    let db_path = unique_temp_path("singular-chat-matrix-db").with_extension("db");
    let url = format!("sqlite://{}", db_path.display());
    let pool = create_sqlite_pool(&url).await.expect("pool");
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("legacy baseline migrations apply");

    let now = "2026-08-13T00:00:00Z";
    let account_id = "matrix-account";
    let merged_project_id = "matrix-merged-project";
    let ambiguous_project_id = "matrix-ambiguous-project";
    let worker_project_id = "matrix-worker-project";
    let turns_project_id = "matrix-turns-project";

    sqlx::query(
        "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES (?, ?, 'test', 'Matrix', ?, ?)",
    )
    .bind(account_id)
    .bind("matrix@example.test")
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("account inserts");

    for (project_id, name) in [
        (merged_project_id, "Merged Project"),
        (ambiguous_project_id, "Ambiguous Project"),
        (worker_project_id, "Worker Project"),
        (turns_project_id, "Turns Project"),
    ] {
        sqlx::query(
            "INSERT INTO project (id, name, settings, workflow_definition, created_at, updated_at)
             VALUES (?, ?, '{}', '{}', ?, ?)",
        )
        .bind(project_id)
        .bind(name)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .expect("project inserts");
        sqlx::query("UPDATE project SET owner_id = ? WHERE id = ?")
            .bind(account_id)
            .bind(project_id)
            .execute(&pool)
            .await
            .expect("project owner assignment");
    }

    for (agent_id, name) in [
        ("matrix-agent-a", "Matrix Agent A"),
        ("matrix-agent-b", "Matrix Agent B"),
        ("matrix-agent-worker", "Matrix Worker"),
    ] {
        sqlx::query(
            "INSERT INTO agent (
                id, name, description, executor_type, model, reasoning_effort,
                permission_policy, prompt_template, capabilities_json, config_json,
                max_concurrent_tasks, status, is_default, paused, owner_id, visibility,
                created_at, updated_at
             ) VALUES (?, ?, NULL, 'null', 'matrix-model', NULL, NULL, NULL, '{}', '{}',
                       1, 'idle', 0, 0, ?, 'account', ?, ?)",
        )
        .bind(agent_id)
        .bind(name)
        .bind(account_id)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .expect("legacy agent inserts");
    }

    sqlx::query(
        "INSERT INTO conversation (
            id, project_id, agent_id, title, status, system_prompt, message_count,
            last_message_at, agent_session_id, version, created_at, updated_at
         ) VALUES (
            'conversation-legacy', ?, 'matrix-agent-a', 'Legacy Conversation', 'active',
            'Legacy instruction', 1, ?, 'opaque-legacy-session', 1, ?, ?
         )",
    )
    .bind(merged_project_id)
    .bind("2026-08-13T00:00:02Z")
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("legacy Conversation inserts");
    sqlx::query(
        "INSERT INTO conversation_message (
            id, conversation_id, role, content, status, sequence, created_at, updated_at
         ) VALUES (
            'legacy-message', 'conversation-legacy', 'user', 'legacy conversation body',
            'complete', 0, '2026-08-13T00:00:02Z', '2026-08-13T00:00:02Z'
         )",
    )
    .execute(&pool)
    .await
    .expect("legacy Conversation message inserts");

    copy_migrations_up_to(70, &migration_dir);
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("Room-era migrations apply");

    for (room_id, project_id, responder_id) in [
        ("room-a", merged_project_id, "matrix-agent-a"),
        ("room-z", merged_project_id, "matrix-agent-a"),
        ("ambiguous-a", ambiguous_project_id, "matrix-agent-a"),
        ("ambiguous-b", ambiguous_project_id, "matrix-agent-b"),
        ("worker-room", worker_project_id, "matrix-agent-worker"),
        ("turn-live", turns_project_id, "matrix-agent-a"),
        ("turn-expired", turns_project_id, "matrix-agent-a"),
        ("turn-exhausted", turns_project_id, "matrix-agent-a"),
    ] {
        sqlx::query(
            "INSERT INTO room (
                id, scope_type, scope_id, owner_user_id, owning_project_id, title, status,
                responder_policy, default_responder_identity_id, history_policy,
                message_count, last_message_at, version, created_at, updated_at
             ) VALUES (?, 'project', ?, ?, ?, ?, 'active', 'explicit_identity', ?,
                       'project_members', 0, NULL, 1, ?, ?)",
        )
        .bind(room_id)
        .bind(project_id)
        .bind(account_id)
        .bind(project_id)
        .bind(format!("{room_id} title"))
        .bind(responder_id)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .expect("pre-singular Room inserts");
    }

    sqlx::query(
        "INSERT INTO room_instruction_revision (
            id, room_id, revision, body, content_guard_json, sensitivity,
            created_by_type, created_by_id, created_at
         ) VALUES ('room-a-instruction', 'room-a', 1, 'Room A instruction', '{}',
                   'internal', 'user', ?, ?)",
    )
    .bind(account_id)
    .bind(now)
    .execute(&pool)
    .await
    .expect("Room instruction inserts");
    sqlx::query(
        "INSERT INTO agent_lcm_timeline (
            id, identity_id, scope_type, scope_id, authorization_revision,
            revision, created_at, updated_at
         ) VALUES ('matrix-room-lcm', 'matrix-agent-a', 'room', 'room-a',
                   'legacy-room-auth', 0, ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("legacy Room LCM timeline inserts");

    let room_messages = [
        (
            "a-message-0",
            "room-a",
            "A first body",
            0_i64,
            "2026-08-13T00:00:01Z",
        ),
        (
            "a-message-1",
            "room-a",
            "A second body",
            1_i64,
            "2026-08-13T00:00:01Z",
        ),
        (
            "z-message-0",
            "room-z",
            "Authorization: Bearer sk-matrix-secret",
            0_i64,
            "2026-08-13T00:00:01Z",
        ),
        (
            "ambiguous-message-a",
            "ambiguous-a",
            "Ambiguous A body",
            0_i64,
            "2026-08-13T00:00:03Z",
        ),
        (
            "ambiguous-message-b",
            "ambiguous-b",
            "Ambiguous B body",
            0_i64,
            "2026-08-13T00:00:03Z",
        ),
        (
            "worker-message",
            "worker-room",
            "Primary Worker transcript",
            0_i64,
            "2026-08-13T00:00:04Z",
        ),
        (
            "live-message",
            "turn-live",
            "Live leased input",
            0_i64,
            "2026-08-13T00:00:05Z",
        ),
        (
            "expired-message",
            "turn-expired",
            "Expired leased input",
            0_i64,
            "2026-08-13T00:00:06Z",
        ),
        (
            "exhausted-message",
            "turn-exhausted",
            "Exhausted leased input",
            0_i64,
            "2026-08-13T00:00:07Z",
        ),
    ];
    for (message_id, room_id, content, sequence, created_at) in room_messages {
        sqlx::query(
            "INSERT INTO room_message (
                id, room_id, author_type, author_id, addressed_identity_id,
                reply_to_message_id, content, content_guard_json, sensitivity, status,
                outcome, model, profile_id, session_id, token_usage_json, duration_ms,
                error, correlation_id, source_event_id, sequence, created_at
             ) VALUES (?, ?, 'user', ?, NULL, NULL, ?, '{}', 'internal', 'complete',
                       NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?, NULL, ?, ?)",
        )
        .bind(message_id)
        .bind(room_id)
        .bind(account_id)
        .bind(content)
        .bind(format!("correlation-{message_id}"))
        .bind(sequence)
        .bind(created_at)
        .execute(&pool)
        .await
        .expect("pre-singular Room message inserts");
    }

    sqlx::query(
        "INSERT INTO project_agent_membership (
            id, project_id, identity_id, role, is_primary, state, created_by_user_id,
            created_at, updated_at
         ) VALUES ('worker-membership', ?, 'matrix-agent-worker', 'worker', 1,
                   'active', ?, ?, ?)",
    )
    .bind(worker_project_id)
    .bind(account_id)
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("primary Worker membership inserts");

    for (job_id, room_id, message_id, status, leased_until, attempt_count, updated_at) in [
        (
            "live-job",
            "turn-live",
            "live-message",
            "leased",
            "2099-01-01T00:00:00Z",
            1_i64,
            now,
        ),
        (
            "expired-job",
            "turn-expired",
            "expired-message",
            "running",
            "2026-08-01T00:00:00Z",
            2_i64,
            now,
        ),
        (
            "exhausted-job",
            "turn-exhausted",
            "exhausted-message",
            "leased",
            "2026-08-01T00:00:00Z",
            3_i64,
            now,
        ),
    ] {
        sqlx::query(
            "INSERT INTO agent_turn_job (
                id, room_id, input_message_id, responder_identity_id, scope_type, scope_id,
                status, dedupe_key, lease_owner, leased_until, attempt_count,
                response_message_id, error, correlation_id, causation_id, causation_depth,
                created_at, updated_at
             ) VALUES (?, ?, ?, 'matrix-agent-a', 'room', ?, ?, ?, 'matrix-worker', ?, ?,
                       NULL, NULL, ?, NULL, 0, ?, ?)",
        )
        .bind(job_id)
        .bind(room_id)
        .bind(message_id)
        .bind(room_id)
        .bind(status)
        .bind(format!("dedupe-{job_id}"))
        .bind(leased_until)
        .bind(attempt_count)
        .bind(format!("correlation-{job_id}"))
        .bind(now)
        .bind(updated_at)
        .execute(&pool)
        .await
        .expect("legacy turn job inserts");
    }

    run_migrations(&pool)
        .await
        .expect("singular Agent Chat migrations apply");

    for table in [
        "room",
        "room_instruction_revision",
        "room_participant",
        "room_message",
        "agent_turn_job",
        "bounded_room_round",
        "bounded_room_round_participant",
        "project_agent_membership",
    ] {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(table)
        .fetch_one(&pool)
        .await
        .expect("live legacy table inspection");
        assert_eq!(
            count, 0,
            "retired runtime table must not remain live: {table}"
        );
    }
    for (table, expected_rows) in [
        ("legacy_room", 9_i64),
        ("legacy_room_instruction_revision", 2_i64),
        ("legacy_room_message", 10_i64),
        ("legacy_agent_turn_job", 3_i64),
        ("legacy_project_agent_membership", 1_i64),
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&pool)
            .await
            .expect("quarantined legacy rows load");
        assert_eq!(
            count, expected_rows,
            "quarantined legacy rows are preserved: {table}"
        );
    }

    let merged_chat_id: String =
        sqlx::query_scalar("SELECT id FROM agent_chat WHERE kind = 'project' AND project_id = ?")
            .bind(merged_project_id)
            .fetch_one(&pool)
            .await
            .expect("merged Project Chat lookup");
    let merged_messages: Vec<(String, String, String, i64, String)> = sqlx::query_as(
        "SELECT id, content, source_room_id, source_sequence, source_type
         FROM agent_chat_message WHERE chat_id = ? ORDER BY sequence ASC",
    )
    .bind(&merged_chat_id)
    .fetch_all(&pool)
    .await
    .expect("merged message lookup");
    assert_eq!(
        merged_messages,
        vec![
            (
                "a-message-0".to_owned(),
                "A first body".to_owned(),
                "room-a".to_owned(),
                0,
                "room".to_owned(),
            ),
            (
                "a-message-1".to_owned(),
                "A second body".to_owned(),
                "room-a".to_owned(),
                1,
                "room".to_owned(),
            ),
            (
                "z-message-0".to_owned(),
                "[protected value redacted during migration]".to_owned(),
                "room-z".to_owned(),
                0,
                "room".to_owned(),
            ),
            (
                "legacy-message".to_owned(),
                "legacy conversation body".to_owned(),
                "conversation-legacy".to_owned(),
                0,
                "room".to_owned(),
            ),
        ]
    );

    let source_ref_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_chat_source_ref
         WHERE chat_id = ? AND source_type = 'room'",
    )
    .bind(&merged_chat_id)
    .fetch_one(&pool)
    .await
    .expect("merged source refs lookup");
    assert_eq!(source_ref_count, 3);
    let lcm_source_ref: (String, String, String) = sqlx::query_as(
        "SELECT source_type, source_scope_type, source_scope_id
         FROM agent_chat_source_ref
         WHERE chat_id = ? AND source_id = 'matrix-room-lcm'",
    )
    .bind(&merged_chat_id)
    .fetch_one(&pool)
    .await
    .expect("legacy LCM source ref lookup");
    assert_eq!(
        lcm_source_ref,
        (
            "lcm_timeline".to_owned(),
            "room".to_owned(),
            "room-a".to_owned()
        )
    );
    let legacy_instruction: (String, String, String) = sqlx::query_as(
        "SELECT source_type, source_id, body FROM agent_chat_instruction_revision
         WHERE chat_id = ? AND source_id = 'conversation-legacy'",
    )
    .bind(&merged_chat_id)
    .fetch_one(&pool)
    .await
    .expect("legacy instruction provenance lookup");
    assert_eq!(
        legacy_instruction,
        (
            "room".to_owned(),
            "conversation-legacy".to_owned(),
            "Legacy instruction".to_owned()
        )
    );

    let protected_session: (String, String) = sqlx::query_as(
        "SELECT room_id, opaque_session_ref FROM protected_legacy_session_ref
         WHERE room_id = 'conversation-legacy'",
    )
    .fetch_one(&pool)
    .await
    .expect("protected legacy session linkage lookup");
    assert_eq!(
        protected_session,
        (
            "conversation-legacy".to_owned(),
            "opaque-legacy-session".to_owned()
        )
    );
    let leaked_session_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_chat_message
         WHERE chat_id = ? AND content LIKE '%opaque-legacy-session%'",
    )
    .bind(&merged_chat_id)
    .fetch_one(&pool)
    .await
    .expect("protected session leak check");
    assert_eq!(leaked_session_count, 0);

    let secret_audit_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM content_guard_audit
         WHERE entity_type = 'agent_chat_message' AND entity_id = 'z-message-0'",
    )
    .fetch_one(&pool)
    .await
    .expect("protected chat audit lookup");
    assert_eq!(secret_audit_count, 1);

    for project_id in [ambiguous_project_id, worker_project_id] {
        let chat_status: String = sqlx::query_scalar(
            "SELECT status FROM agent_chat WHERE kind = 'project' AND project_id = ?",
        )
        .bind(project_id)
        .fetch_one(&pool)
        .await
        .expect("setup-required Project Chat lookup");
        assert_eq!(chat_status, "agent_setup_required");
        let binding: (String, Option<String>, Option<String>) = sqlx::query_as(
            "SELECT state, identity_id, profile_id FROM project_agent_binding WHERE project_id = ?",
        )
        .bind(project_id)
        .fetch_one(&pool)
        .await
        .expect("setup-required Project binding lookup");
        assert_eq!(binding, ("agent_setup_required".to_owned(), None, None));
    }
    let worker_binding_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_agent_binding
         WHERE project_id = ? AND identity_id = 'matrix-agent-worker' AND state = 'active'",
    )
    .bind(worker_project_id)
    .fetch_one(&pool)
    .await
    .expect("primary Worker binding lookup");
    assert_eq!(worker_binding_count, 0);

    type MigratedTurnState = (String, String, Option<String>, Option<String>, i64);
    let turn_states: Vec<MigratedTurnState> = sqlx::query_as(
        "SELECT id, status, error_code, lease_owner, attempt_count
             FROM agent_chat_turn_job WHERE id IN ('live-job', 'expired-job', 'exhausted-job')
             ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("migrated turn state lookup");
    assert_eq!(
        turn_states,
        vec![
            (
                "exhausted-job".to_owned(),
                "failed".to_owned(),
                Some("retry_exhausted".to_owned()),
                None,
                3,
            ),
            (
                "expired-job".to_owned(),
                "retry_wait".to_owned(),
                Some("lease_expired_during_migration".to_owned()),
                None,
                2,
            ),
            (
                "live-job".to_owned(),
                "leased".to_owned(),
                None,
                Some("matrix-worker".to_owned()),
                1,
            ),
        ]
    );
    let canonical_scope: (String, String) = sqlx::query_as(
        "SELECT canonical_scope_type, canonical_scope_id FROM agent_chat_turn_job
         WHERE id = 'live-job'",
    )
    .fetch_one(&pool)
    .await
    .expect("canonical turn scope lookup");
    assert_eq!(canonical_scope.0, "agent_chat");
    assert_eq!(
        canonical_scope.1,
        merged_chat_id_for(&pool, turns_project_id).await
    );

    let foreign_key_violations: Vec<(String, i64, String, i64)> =
        sqlx::query_as("PRAGMA foreign_key_check")
            .fetch_all(&pool)
            .await
            .expect("foreign key check runs");
    assert!(foreign_key_violations.is_empty());

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(migration_dir);
}

async fn merged_chat_id_for(pool: &db::SqlitePool, project_id: &str) -> String {
    sqlx::query_scalar("SELECT id FROM agent_chat WHERE kind = 'project' AND project_id = ?")
        .bind(project_id)
        .fetch_one(pool)
        .await
        .expect("Project Chat id lookup")
}

#[tokio::test]
async fn singular_agent_chat_empty_database_has_no_synthetic_records() {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");

    for (table, predicate) in [
        ("user", "1 = 1"),
        ("project", "1 = 1"),
        ("agent_identity", "1 = 1"),
        ("agent_profile", "1 = 1"),
        ("account_main_agent_binding", "1 = 1"),
        ("project_agent_binding", "1 = 1"),
        ("agent_chat", "1 = 1"),
        ("agent_chat_message", "1 = 1"),
        ("agent_chat_turn_job", "1 = 1"),
        ("agent_chat_source_ref", "1 = 1"),
    ] {
        let sql = format!("SELECT COUNT(*) FROM {table} WHERE {predicate}");
        let count: i64 = sqlx::query_scalar(&sql)
            .fetch_one(&pool)
            .await
            .expect("empty table lookup");
        assert_eq!(count, 0, "empty migration must not synthesize {table} rows");
    }
}

#[tokio::test]
async fn v135_execution_usage_import_preserves_legacy_rows_and_provenance() {
    let migration_dir = unique_temp_path("v135-legacy-migrations");
    fs::create_dir_all(&migration_dir).expect("temp migration dir creates");
    copy_migrations_up_to(134, &migration_dir);

    let db_path = unique_temp_path("v135-legacy-db").with_extension("db");
    let url = format!("sqlite://{}", db_path.display());
    let pool = create_sqlite_pool(&url).await.expect("pool");
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("pre-V135 migrations apply");

    let now = "2026-09-07T00:00:00Z";
    let owner_id = "v135-owner";
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES (?, 'v135@example.test', 'not-a-password', 'V135 owner', ?, ?)",
    )
    .bind(owner_id)
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("owner inserts");

    for (project_id, project_name, project_owner_id) in [
        ("v135-owned", "Owned", Some(owner_id)),
        ("v135-ownerless", "Ownerless", None),
        (
            "v135-dangling-owner",
            "Dangling owner",
            Some("missing-v135-owner"),
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
         ) VALUES (
            'v135-deleted-agent', 'Deleted V135 agent', NULL, 1, 30, 3, 'idle',
            0, 0, 1, ?, ?, ?, 'account'
         )",
    )
    .bind(now)
    .bind(now)
    .bind(owner_id)
    .execute(&pool)
    .await
    .expect("agent identity inserts");
    sqlx::query(
        "INSERT INTO agent_profile (
            id, identity_id, backend_kind, executor_type, provider, model,
            capabilities_json, tool_policy_json, config_json, version,
            created_at, updated_at
         ) VALUES (
            'v135-deleted-profile', 'v135-deleted-agent', 'native', 'codex',
            'openai', 'gpt-5', '[]', '{}', '{}', 1, ?, ?
         )",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("agent profile inserts");
    sqlx::query(
        "UPDATE agent_identity SET selected_profile_id = 'v135-deleted-profile'
         WHERE id = 'v135-deleted-agent'",
    )
    .execute(&pool)
    .await
    .expect("agent profile selection");

    for (execution_id, task_id, project_id, agent_id) in [
        (
            "v135-execution-valid",
            "v135-task-valid",
            "v135-owned",
            Some("v135-deleted-agent"),
        ),
        (
            "v135-execution-zero-null",
            "v135-task-zero-null",
            "v135-owned",
            None,
        ),
        (
            "v135-execution-zero-cost",
            "v135-task-zero-cost",
            "v135-owned",
            None,
        ),
        (
            "v135-execution-ownerless",
            "v135-task-ownerless",
            "v135-ownerless",
            None,
        ),
        (
            "v135-execution-dangling-owner",
            "v135-task-dangling-owner",
            "v135-dangling-owner",
            None,
        ),
        (
            "v135-execution-empty-identities",
            "v135-task-empty-identities",
            "v135-owned",
            None,
        ),
        (
            "v135-execution-malformed",
            "v135-task-malformed",
            "v135-owned",
            None,
        ),
        (
            "v135-execution-negative-cost",
            "v135-task-negative-cost",
            "v135-owned",
            None,
        ),
    ] {
        sqlx::query(
            "INSERT INTO task (
                id, project_id, repo_id, title, status, created_at, updated_at
             ) VALUES (?, ?, ?, ?, 'done', ?, ?)",
        )
        .bind(task_id)
        .bind(project_id)
        .bind(format!("{project_id}-repo"))
        .bind(format!("{task_id} title"))
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .expect("task inserts");
        sqlx::query(
            "INSERT INTO execution (
                id, task_id, agent_id, role, status, created_at, updated_at
             ) VALUES (?, ?, ?, 'executor', 'completed', ?, ?)",
        )
        .bind(execution_id)
        .bind(task_id)
        .bind(agent_id)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .expect("execution inserts");
    }

    sqlx::query(
        "INSERT INTO execution_usage (
            id, execution_id, provider, model, input_tokens, output_tokens,
            cache_read_tokens, cache_write_tokens, cost_usd, created_at
         ) VALUES
            ('legacy-valid', 'v135-execution-valid', 'openai', 'gpt-5',
             120, 34, 5, 6, 0.123456, '2026-09-07T00:00:01Z'),
            ('legacy-zero-null', 'v135-execution-zero-null', 'openai', 'gpt-5-mini',
             0, 0, 0, 0, NULL, '2026-09-07T00:00:02Z'),
            ('legacy-zero-cost', 'v135-execution-zero-cost', 'openai', 'gpt-5-zero',
             0, 0, 0, 0, 0.0, '2026-09-07T00:00:03Z'),
            ('legacy-ownerless', 'v135-execution-ownerless', 'openai', 'gpt-ownerless',
             3, 4, 0, 0, NULL, '2026-09-07T00:00:04Z'),
            ('legacy-dangling-owner', 'v135-execution-dangling-owner',
             'openrouter', 'gpt-dangling', 7, 8, 9, 10, NULL,
             '2026-09-07T00:00:05Z'),
            ('legacy-empty-identities', 'v135-execution-empty-identities', '', '',
             1, 0, 0, 0, NULL, '2026-09-07T00:00:06Z'),
            ('legacy-malformed', 'v135-execution-malformed', 'openai', 'gpt-malformed',
             1.5, -2, 'not-an-integer', 0, 'not-a-cost', '2026-09-07T00:00:07Z'),
            ('legacy-negative-cost', 'v135-execution-negative-cost', 'openai', 'gpt-negative',
             1, 2, 3, 4, -0.25, '2026-09-07T00:00:08Z')",
    )
    .execute(&pool)
    .await
    .expect("legacy execution usage inserts");

    let source_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution_usage")
        .fetch_one(&pool)
        .await
        .expect("pre-migration source row count");
    assert_eq!(source_count, 8);
    // V135 drops execution_usage after its in-transaction copy/validation.
    // Keep a test-only snapshot so the post-migration assertions can still
    // compare every raw V022 representation without treating the retired
    // source table as an authority.
    sqlx::query(
        "CREATE TABLE v135_test_execution_usage_snapshot AS
         SELECT * FROM execution_usage",
    )
    .execute(&pool)
    .await
    .expect("execution usage test snapshot creates");

    run_migrations(&pool).await.expect("V135 migration applies");

    let source_table_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master
         WHERE type = 'table' AND name = 'execution_usage'",
    )
    .fetch_one(&pool)
    .await
    .expect("retired source table lookup");
    assert_eq!(source_table_count, 0, "V135 removes the legacy authority");
    for (table, predicate) in [
        (
            "pricing_selection",
            "provenance_kind = 'legacy_execution_aggregate'",
        ),
        (
            "usage_invocation",
            "provenance_kind = 'legacy_execution_aggregate'",
        ),
        (
            "usage_event",
            "provenance_kind = 'legacy_execution_aggregate' AND report_mode = 'legacy_aggregate'",
        ),
    ] {
        let count: i64 =
            sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table} WHERE {predicate}"))
                .fetch_one(&pool)
                .await
                .expect("legacy target row count");
        assert_eq!(count, source_count, "one {table} row per V022 row");
    }

    let raw_preserved: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM v135_test_execution_usage_snapshot eu
         JOIN execution e ON e.id = eu.execution_id
         JOIN task t ON t.id = e.task_id
         JOIN project p ON p.id = t.project_id
         JOIN usage_event ue ON ue.id = CAST(eu.id AS TEXT)
         WHERE ue.legacy_source_table IS 'execution_usage'
           AND ue.legacy_source_id IS CAST(eu.id AS TEXT)
           AND ue.source_report_id IS CAST(eu.id AS TEXT)
           AND ue.source_id IS CAST(e.id AS TEXT)
           AND ue.execution_id IS CAST(e.id AS TEXT)
           AND ue.task_id IS CAST(t.id AS TEXT)
           AND ue.project_id IS CAST(p.id AS TEXT)
           AND ue.occurred_at IS CAST(eu.created_at AS TEXT)
           AND ue.legacy_cost_usd_raw IS quote(eu.cost_usd)
           AND ue.legacy_created_at_raw IS quote(eu.created_at)
           AND ue.legacy_project_owner_raw IS quote(p.owner_id)
           AND ue.legacy_provider_sqlite_type IS typeof(eu.provider)
           AND ue.legacy_provider_sql_literal IS quote(eu.provider)
           AND ue.legacy_model_sqlite_type IS typeof(eu.model)
           AND ue.legacy_model_sql_literal IS quote(eu.model)
           AND ue.legacy_provider_raw IS CASE
               WHEN typeof(eu.provider) = 'text' THEN eu.provider ELSE NULL END
           AND ue.legacy_model_raw IS CASE
               WHEN typeof(eu.model) = 'text' THEN eu.model ELSE NULL END
           AND json_extract(ue.legacy_counter_values_json,
               '$.input_tokens.sqlite_type') IS typeof(eu.input_tokens)
           AND json_extract(ue.legacy_counter_values_json,
               '$.input_tokens.sql_literal') IS quote(eu.input_tokens)
           AND json_extract(ue.legacy_counter_values_json,
               '$.output_tokens.sqlite_type') IS typeof(eu.output_tokens)
           AND json_extract(ue.legacy_counter_values_json,
               '$.output_tokens.sql_literal') IS quote(eu.output_tokens)
           AND json_extract(ue.legacy_counter_values_json,
               '$.cache_read_tokens.sqlite_type') IS typeof(eu.cache_read_tokens)
           AND json_extract(ue.legacy_counter_values_json,
               '$.cache_read_tokens.sql_literal') IS quote(eu.cache_read_tokens)
           AND json_extract(ue.legacy_counter_values_json,
               '$.cache_write_tokens.sqlite_type') IS typeof(eu.cache_write_tokens)
           AND json_extract(ue.legacy_counter_values_json,
               '$.cache_write_tokens.sql_literal') IS quote(eu.cache_write_tokens)",
    )
    .fetch_one(&pool)
    .await
    .expect("raw legacy provenance validation");
    assert_eq!(raw_preserved, source_count);

    // INSERT OR REPLACE is implemented by SQLite as DELETE followed by
    // INSERT.  With recursive triggers enabled on every pool connection,
    // that path must honor V135's append-only guards rather than silently
    // replacing an immutable ledger row.
    let replace_event = sqlx::query(
        "INSERT OR REPLACE INTO usage_event
         SELECT * FROM usage_event WHERE id = 'legacy-valid'",
    )
    .execute(&pool)
    .await;
    assert!(
        replace_event.is_err(),
        "INSERT OR REPLACE cannot replace an immutable usage event"
    );
    let retained_event_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM usage_event WHERE id = 'legacy-valid'")
            .fetch_one(&pool)
            .await
            .expect("immutable usage event remains queryable");
    assert_eq!(retained_event_count, 1);

    let snapshot_sha = "a".repeat(64);
    sqlx::query(
        "INSERT INTO pricing_catalog_snapshot (
            id, source_kind, source_url, payload_sha256, parser_revision,
            revision_digest, payload_json, fetched_at, created_at
         ) VALUES (?, 'models_dev_catalog', 'https://models.dev/api.json', ?,
                   'v135-test-parser', 'v135-test-digest', '{\"version\":1}', ?, ?)",
    )
    .bind("v135-replace-snapshot")
    .bind(&snapshot_sha)
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("immutable snapshot fixture inserts");
    let replace_snapshot = sqlx::query(
        "INSERT OR REPLACE INTO pricing_catalog_snapshot (
            id, source_kind, source_url, payload_sha256, parser_revision,
            revision_digest, payload_json, fetched_at, created_at
         ) VALUES (?, 'models_dev_catalog', 'https://models.dev/api.json', ?,
                   'v135-test-parser', 'v135-test-digest', '{\"version\":2}', ?, ?)",
    )
    .bind("v135-replace-snapshot")
    .bind(&snapshot_sha)
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await;
    assert!(
        replace_snapshot.is_err(),
        "INSERT OR REPLACE cannot replace an immutable catalog snapshot"
    );
    let retained_payload: String = sqlx::query_scalar(
        "SELECT payload_json FROM pricing_catalog_snapshot
         WHERE id = 'v135-replace-snapshot'",
    )
    .fetch_one(&pool)
    .await
    .expect("immutable snapshot remains queryable");
    assert_eq!(retained_payload, r#"{"version":1}"#);

    let expected = [
        (
            "legacy-valid",
            "v135-execution-valid",
            "v135-task-valid",
            "v135-owned",
            Some(owner_id),
            Some("openai"),
            Some("gpt-5"),
            "metered",
            "provider_reported",
            None,
            [Some(120), Some(34), Some(5), Some(6)],
            Some(0.123456),
        ),
        (
            "legacy-zero-null",
            "v135-execution-zero-null",
            "v135-task-zero-null",
            "v135-owned",
            Some(owner_id),
            Some("openai"),
            Some("gpt-5-mini"),
            "metered",
            "none",
            Some("missing_rate"),
            [Some(0), Some(0), Some(0), Some(0)],
            None,
        ),
        (
            "legacy-zero-cost",
            "v135-execution-zero-cost",
            "v135-task-zero-cost",
            "v135-owned",
            Some(owner_id),
            Some("openai"),
            Some("gpt-5-zero"),
            "metered",
            "provider_reported",
            None,
            [Some(0), Some(0), Some(0), Some(0)],
            Some(0.0),
        ),
        (
            "legacy-ownerless",
            "v135-execution-ownerless",
            "v135-task-ownerless",
            "v135-ownerless",
            None,
            Some("openai"),
            Some("gpt-ownerless"),
            "metered",
            "none",
            Some("missing_rate"),
            [Some(3), Some(4), Some(0), Some(0)],
            None,
        ),
        (
            "legacy-dangling-owner",
            "v135-execution-dangling-owner",
            "v135-task-dangling-owner",
            "v135-dangling-owner",
            None,
            Some("openrouter"),
            Some("gpt-dangling"),
            "metered",
            "none",
            Some("missing_rate"),
            [Some(7), Some(8), Some(9), Some(10)],
            None,
        ),
        (
            "legacy-empty-identities",
            "v135-execution-empty-identities",
            "v135-task-empty-identities",
            "v135-owned",
            Some(owner_id),
            None,
            None,
            "metered",
            "none",
            Some("missing_provider"),
            [Some(1), Some(0), Some(0), Some(0)],
            None,
        ),
        (
            "legacy-malformed",
            "v135-execution-malformed",
            "v135-task-malformed",
            "v135-owned",
            Some(owner_id),
            Some("openai"),
            Some("gpt-malformed"),
            "unmetered",
            "none",
            Some("invalid_legacy_usage"),
            [None, None, None, None],
            None,
        ),
        (
            "legacy-negative-cost",
            "v135-execution-negative-cost",
            "v135-task-negative-cost",
            "v135-owned",
            Some(owner_id),
            Some("openai"),
            Some("gpt-negative"),
            "metered",
            "none",
            Some("invalid_legacy_usage"),
            [Some(1), Some(2), Some(3), Some(4)],
            None,
        ),
    ];

    for (
        source_id,
        execution_id,
        task_id,
        project_id,
        expected_owner,
        expected_provider,
        expected_model,
        expected_telemetry,
        expected_cost_kind,
        expected_reason,
        expected_counters,
        expected_reported_cost,
    ) in expected
    {
        let selection_id = format!("legacy-execution-selection:{source_id}");
        let invocation_id = format!("legacy-execution-invocation:{source_id}");
        type LegacySelectionRow = (
            String,
            Option<String>,
            String,
            Option<String>,
            String,
            String,
            String,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
        );
        let selection: LegacySelectionRow = sqlx::query_as(
            "SELECT id, invocation_id, selection_status, owner_user_id,
                    project_id, source_id, execution_id, task_id,
                    admitted_provider_id, admitted_model_id, runtime_model
             FROM pricing_selection WHERE id = ?",
        )
        .bind(&selection_id)
        .fetch_one(&pool)
        .await
        .expect("legacy selection lookup");
        assert_eq!(selection.0, selection_id);
        assert_eq!(selection.1.as_deref(), Some(invocation_id.as_str()));
        assert_eq!(selection.2, "unpriced");
        assert_eq!(selection.3.as_deref(), expected_owner);
        assert_eq!(selection.4, project_id);
        assert_eq!(selection.5, execution_id);
        assert_eq!(selection.6, execution_id);
        assert_eq!(selection.7, task_id);
        assert_eq!(selection.8.as_deref(), expected_provider);
        assert_eq!(selection.9.as_deref(), expected_model);
        assert_eq!(selection.10.as_deref(), expected_model);

        type LegacyInvocationRow = (
            String,
            String,
            String,
            String,
            Option<String>,
            String,
            String,
            String,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        );
        let invocation: LegacyInvocationRow = sqlx::query_as(
            "SELECT id, pricing_selection_id, lifecycle, telemetry_state,
                    owner_user_id, project_id, source_id, execution_id, task_id,
                    admitted_provider_id, admitted_model_id, admitted_runtime_model,
                    terminal_reason
             FROM usage_invocation WHERE id = ?",
        )
        .bind(&invocation_id)
        .fetch_one(&pool)
        .await
        .expect("legacy invocation lookup");
        assert_eq!(invocation.0, invocation_id);
        assert_eq!(invocation.1, selection_id);
        assert_eq!(invocation.2, "settled");
        assert_eq!(invocation.3, expected_telemetry);
        assert_eq!(invocation.4.as_deref(), expected_owner);
        assert_eq!(invocation.5, project_id);
        assert_eq!(invocation.6, execution_id);
        assert_eq!(invocation.7, execution_id);
        assert_eq!(invocation.8, task_id);
        assert_eq!(invocation.9.as_deref(), expected_provider);
        assert_eq!(invocation.10.as_deref(), expected_model);
        assert_eq!(invocation.11.as_deref(), expected_model);
        assert_eq!(
            invocation.12.as_deref(),
            (expected_telemetry == "unmetered")
                .then_some("legacy row retained with invalid or non-integer telemetry")
        );

        type LegacyEventIdentityRow = (
            String,
            String,
            Option<String>,
            String,
            String,
            String,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            String,
        );
        let event_identity: LegacyEventIdentityRow = sqlx::query_as(
            "SELECT id, invocation_id, owner_user_id, project_id, source_id,
                    execution_id, task_id, provider_id, model_id, runtime_model,
                    telemetry_state
             FROM usage_event WHERE id = ?",
        )
        .bind(source_id)
        .fetch_one(&pool)
        .await
        .expect("legacy usage event lookup");
        assert_eq!(event_identity.0, source_id);
        assert_eq!(event_identity.1, invocation_id);
        assert_eq!(event_identity.2.as_deref(), expected_owner);
        assert_eq!(event_identity.3, project_id);
        assert_eq!(event_identity.4, execution_id);
        assert_eq!(event_identity.5, execution_id);
        assert_eq!(event_identity.6, task_id);
        assert_eq!(event_identity.7.as_deref(), expected_provider);
        assert_eq!(event_identity.8.as_deref(), expected_model);
        assert_eq!(event_identity.9.as_deref(), expected_model);
        assert_eq!(event_identity.10, expected_telemetry);

        type LegacyEventTelemetryCostRow = (
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            String,
            Option<String>,
            Option<f64>,
            Option<f64>,
        );
        let event_telemetry_cost: LegacyEventTelemetryCostRow = sqlx::query_as(
            "SELECT input_tokens, output_tokens, cache_read_tokens,
                    cache_write_tokens, cost_kind, coverage_reason_code,
                    legacy_reported_cost_usd,
                    CAST(provider_reported_nano_usd AS REAL)
             FROM usage_event WHERE id = ?",
        )
        .bind(source_id)
        .fetch_one(&pool)
        .await
        .expect("legacy usage telemetry lookup");
        assert_eq!(
            [
                event_telemetry_cost.0,
                event_telemetry_cost.1,
                event_telemetry_cost.2,
                event_telemetry_cost.3,
            ],
            expected_counters
        );
        assert_eq!(event_telemetry_cost.4, expected_cost_kind);
        assert_eq!(event_telemetry_cost.5.as_deref(), expected_reason);
        assert_eq!(event_telemetry_cost.6, expected_reported_cost);
        assert_eq!(event_telemetry_cost.7, None);
    }

    sqlx::query("DELETE FROM agent_identity WHERE id = 'v135-deleted-agent'")
        .execute(&pool)
        .await
        .expect("deleted agent cleanup");
    let deleted_profile_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_profile WHERE id = 'v135-deleted-profile'")
            .fetch_one(&pool)
            .await
            .expect("deleted profile lookup");
    assert_eq!(deleted_profile_count, 0);
    let deleted_agent_reference: Option<String> =
        sqlx::query_scalar("SELECT agent_id FROM execution WHERE id = 'v135-execution-valid'")
            .fetch_one(&pool)
            .await
            .expect("deleted agent execution reference lookup");
    assert_eq!(deleted_agent_reference, None);
    let migrated_identity_provenance: (Option<String>, Option<String>) =
        sqlx::query_as("SELECT agent_id, profile_id FROM usage_event WHERE id = 'legacy-valid'")
            .fetch_one(&pool)
            .await
            .expect("migrated identity provenance lookup");
    assert_eq!(migrated_identity_provenance, (None, None));
    let retained_after_agent_delete: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM usage_event
         WHERE provenance_kind = 'legacy_execution_aggregate'",
    )
    .fetch_one(&pool)
    .await
    .expect("usage retention after agent delete");
    assert_eq!(retained_after_agent_delete, source_count);

    sqlx::query("DROP TABLE v135_test_execution_usage_snapshot")
        .execute(&pool)
        .await
        .expect("execution usage test snapshot drops");

    let foreign_key_violations: Vec<(String, i64, String, i64)> =
        sqlx::query_as("PRAGMA foreign_key_check")
            .fetch_all(&pool)
            .await
            .expect("foreign key check runs");
    assert!(foreign_key_violations.is_empty());

    let rollback_db_path = unique_temp_path("v135-legacy-rollback-db").with_extension("db");
    let rollback_url = format!("sqlite://{}", rollback_db_path.display());
    let rollback_pool = create_sqlite_pool(&rollback_url)
        .await
        .expect("rollback pool");
    run_migrations_from(&rollback_pool, &migration_dir)
        .await
        .expect("rollback pre-V135 migrations apply");
    sqlx::query("CREATE TABLE pricing_selection (id TEXT PRIMARY KEY, sentinel TEXT NOT NULL)")
        .execute(&rollback_pool)
        .await
        .expect("deterministic target conflict setup");
    sqlx::query("INSERT INTO pricing_selection (id, sentinel) VALUES ('conflict', 'still-here')")
        .execute(&rollback_pool)
        .await
        .expect("deterministic target conflict sentinel");
    assert!(run_migrations(&rollback_pool).await.is_err());
    let v135_migration_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM _migration WHERE version = 135")
            .fetch_one(&rollback_pool)
            .await
            .expect("rollback migration marker lookup");
    assert_eq!(v135_migration_count, 0);
    for table in [
        "pricing_catalog_snapshot",
        "pricing_catalog_state",
        "pricing_rate_revision",
        "usage_invocation",
        "usage_event",
    ] {
        let object_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(table)
        .fetch_one(&rollback_pool)
        .await
        .expect("rollback table lookup");
        assert_eq!(object_count, 0, "failed V135 leaves no partial {table}");
    }
    let source_table_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master
         WHERE type = 'table' AND name = 'execution_usage'",
    )
    .fetch_one(&rollback_pool)
    .await
    .expect("rolled-back source table lookup");
    assert_eq!(
        source_table_count, 1,
        "failed V135 preserves the source table"
    );
    let conflict_sentinel_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM pricing_selection WHERE sentinel = 'still-here'")
            .fetch_one(&rollback_pool)
            .await
            .expect("conflict sentinel survives rollback");
    assert_eq!(conflict_sentinel_count, 1);

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(rollback_db_path);
    let _ = fs::remove_dir_all(migration_dir);
}

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
