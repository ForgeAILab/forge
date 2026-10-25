//! Repository, agent identity, membership, and task scope migrations.

use super::*;

#[tokio::test]
async fn repo_remote_url_migration_normalizes_blanks_and_preserves_references() {
    let migration_dir = unique_temp_path("repo-remote-migrations");
    fs::create_dir_all(&migration_dir).expect("migration directory creates");
    copy_migrations_up_to(149, &migration_dir);
    let db_path = unique_temp_path("repo-remote-db").with_extension("db");
    let pool = create_sqlite_pool(&format!("sqlite://{}", db_path.display()))
        .await
        .expect("pool creates");
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("pre-V150 migrations apply");
    let now = "2026-09-30T00:00:00Z";
    sqlx::query(
        "INSERT INTO project (id, name, settings, workflow_definition, created_at, updated_at)
         VALUES ('project-remote-migration', 'Local Project', '{}', '{}', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("project inserts");
    let remotes = [
        ("repo-empty", ""),
        ("repo-spaces", "   "),
        ("repo-whitespace", " \t\r\n"),
        ("repo-unicode", "\u{2003}\u{a0}"),
        ("repo-remote", " https://example.com/repo.git "),
    ];
    for (id, remote) in remotes {
        sqlx::query(
            "INSERT INTO repo (id, project_id, name, remote_url, local_path, work_mode,
                               default_branch, created_at, updated_at)
             VALUES (?, 'project-remote-migration', 'Local Repo', ?, '/tmp/local-repo',
                     'direct_merge', 'main', ?, ?)",
        )
        .bind(id)
        .bind(remote)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .expect("repo inserts");
    }
    sqlx::query(
        "UPDATE project SET primary_repo_id = 'repo-empty' WHERE id = 'project-remote-migration'",
    )
    .execute(&pool)
    .await
    .expect("primary repo selects");
    sqlx::query(
        "INSERT INTO task (id, project_id, title, task_type, status, created_at, updated_at)
         VALUES ('task-remote-migration', 'project-remote-migration', 'Local Task', 'task', 'done', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("task inserts");
    sqlx::query(
        "INSERT INTO workspace (id, task_id, repo_id, worktree_path, branch, status, created_at, updated_at)
         VALUES ('workspace-remote-migration', 'task-remote-migration', 'repo-empty',
                 '/tmp/local-worktree', 'forge/task-remote-migration', 'ready', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("workspace inserts");
    sqlx::query(
        "INSERT INTO pr_provider_config (id, repo_id, provider_type, created_at, updated_at)
         VALUES ('provider-remote-migration', 'repo-remote', 'github', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("provider configuration inserts");

    run_migrations(&pool).await.expect("V150 applies");
    run_migrations(&pool)
        .await
        .expect("migration restart succeeds");
    for (id, remote) in remotes {
        let row = sqlx::query("SELECT * FROM repo WHERE id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await
            .expect("repo survives");
        let stored: Option<String> = row.try_get("remote_url").expect("remote loads");
        assert_eq!(
            stored.as_deref(),
            if id == "repo-remote" {
                Some(remote)
            } else {
                None
            }
        );
        assert_eq!(
            row.try_get::<String, _>("project_id").unwrap(),
            "project-remote-migration"
        );
        assert_eq!(row.try_get::<String, _>("name").unwrap(), "Local Repo");
        assert_eq!(
            row.try_get::<String, _>("local_path").unwrap(),
            "/tmp/local-repo"
        );
        assert_eq!(
            row.try_get::<String, _>("work_mode").unwrap(),
            "direct_merge"
        );
        assert_eq!(row.try_get::<String, _>("default_branch").unwrap(), "main");
        assert_eq!(row.try_get::<String, _>("created_at").unwrap(), now);
        assert_eq!(row.try_get::<String, _>("updated_at").unwrap(), now);
    }
    let references: (String, String, String) = sqlx::query_as(
        "SELECT p.primary_repo_id, w.repo_id, config.repo_id
         FROM project p, workspace w, pr_provider_config config
         WHERE p.id = 'project-remote-migration' AND w.id = 'workspace-remote-migration'
           AND config.id = 'provider-remote-migration'",
    )
    .fetch_one(&pool)
    .await
    .expect("repository references survive");
    assert_eq!(
        references,
        (
            "repo-empty".to_owned(),
            "repo-empty".to_owned(),
            "repo-remote".to_owned()
        )
    );
    assert!(sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&pool)
        .await
        .expect("foreign keys check")
        .is_empty());
    let foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(&pool)
        .await
        .expect("foreign key setting loads");
    assert_eq!(foreign_keys, 1);
    sqlx::query("UPDATE repo SET remote_url = NULL WHERE id = 'repo-remote'")
        .execute(&pool)
        .await
        .expect("remote column accepts NULL");
    pool.close().await;
    fs::remove_file(db_path).expect("database removes");
    fs::remove_dir_all(migration_dir).expect("migration directory removes");
}

#[tokio::test]
async fn solo_ready_permission_migration_repairs_only_the_exact_legacy_ceiling() {
    const LEGACY: &str = r#"{"permissions":["read_project","read_agent_chat","read_memory","propose_message","propose_project"]}"#;
    const READY: &str = r#"{"permissions":["read_project","read_agent_chat","read_task","read_memory","propose_task","propose_project","propose_message","propose_review","propose_commitment","propose_memory","propose_decision","propose_session"]}"#;

    let migration_dir = unique_temp_path("solo-ready-permission-migration");
    fs::create_dir_all(&migration_dir).expect("temp migration dir creates");
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("migrations/V141__solo_project_agent_ready_permissions.sql");
    fs::copy(
        source,
        migration_dir.join("V141__solo_project_agent_ready_permissions.sql"),
    )
    .expect("Solo migration copies");

    let db_path = unique_temp_path("solo-ready-permission-db").with_extension("db");
    let url = format!("sqlite://{}", db_path.display());
    let pool = create_sqlite_pool(&url).await.expect("pool");
    sqlx::query(
        "CREATE TABLE project (
             id TEXT PRIMARY KEY,
             settings TEXT NOT NULL,
             charter_status TEXT NOT NULL,
             charter_setup_required INTEGER NOT NULL
         );
         CREATE TABLE project_agent_binding (
             id TEXT PRIMARY KEY,
             project_id TEXT NOT NULL,
             state TEXT NOT NULL,
             charter_setup_required INTEGER NOT NULL,
             permission_ceiling_json TEXT NOT NULL,
             version INTEGER NOT NULL,
             updated_at TEXT NOT NULL
         );",
    )
    .execute(&pool)
    .await
    .expect("minimal pre-V141 schema creates");

    for (project_id, settings, project_setup, binding_state, binding_setup, ceiling) in [
        (
            "solo-ready",
            r#"{"forge_solo":{"contract_revision":"forge.solo.bootstrap/v1"}}"#,
            0,
            "active",
            0,
            LEGACY,
        ),
        ("ordinary-ready", "{}", 0, "active", 0, LEGACY),
        ("solo-setup", r#"{"forge_solo":{}}"#, 1, "active", 1, LEGACY),
        (
            "solo-setup-mismatched-binding",
            r#"{"forge_solo":{}}"#,
            1,
            "active",
            0,
            LEGACY,
        ),
        (
            "solo-ready-mismatched-binding",
            r#"{"forge_solo":{}}"#,
            0,
            "active",
            1,
            LEGACY,
        ),
        (
            "solo-replaced",
            r#"{"forge_solo":{}}"#,
            1,
            "replaced",
            1,
            LEGACY,
        ),
        (
            "solo-custom",
            r#"{"forge_solo":{}}"#,
            0,
            "active",
            0,
            r#"{"permissions":["read_project"]}"#,
        ),
    ] {
        sqlx::query(
            "INSERT INTO project (
                 id, settings, charter_status, charter_setup_required
             ) VALUES (?, ?, ?, ?)",
        )
        .bind(project_id)
        .bind(settings)
        .bind(if project_setup == 0 {
            "charter_backed"
        } else {
            "legacy_unverified"
        })
        .bind(project_setup)
        .execute(&pool)
        .await
        .expect("migration fixture Project inserts");
        sqlx::query(
            "INSERT INTO project_agent_binding (
                 id, project_id, state, charter_setup_required,
                 permission_ceiling_json, version, updated_at
             ) VALUES (?, ?, ?, ?, ?, 7, '2026-09-10T00:00:00Z')",
        )
        .bind(format!("binding-{project_id}"))
        .bind(project_id)
        .bind(binding_state)
        .bind(binding_setup)
        .bind(ceiling)
        .execute(&pool)
        .await
        .expect("migration fixture binding inserts");
    }

    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("V141 applies");

    let repaired: (String, i64) = sqlx::query_as(
        "SELECT permission_ceiling_json, version
         FROM project_agent_binding WHERE project_id = 'solo-ready'",
    )
    .fetch_one(&pool)
    .await
    .expect("repaired Solo binding reads");
    assert_eq!(repaired, (READY.to_owned(), 8));

    let repaired_setup: (String, i64) = sqlx::query_as(
        "SELECT permission_ceiling_json, version
         FROM project_agent_binding WHERE project_id = 'solo-setup'",
    )
    .fetch_one(&pool)
    .await
    .expect("repaired in-flight Solo adoption binding reads");
    assert_eq!(repaired_setup, (READY.to_owned(), 8));

    for (project_id, expected) in [
        ("ordinary-ready", LEGACY),
        ("solo-setup-mismatched-binding", LEGACY),
        ("solo-ready-mismatched-binding", LEGACY),
        ("solo-replaced", LEGACY),
        ("solo-custom", r#"{"permissions":["read_project"]}"#),
    ] {
        let unchanged: (String, i64) = sqlx::query_as(
            "SELECT permission_ceiling_json, version
             FROM project_agent_binding WHERE project_id = ?",
        )
        .bind(project_id)
        .fetch_one(&pool)
        .await
        .expect("unmatched binding reads");
        assert_eq!(unchanged, (expected.to_owned(), 7), "{project_id}");
    }

    pool.close().await;
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(migration_dir);
}

#[tokio::test]
async fn project_owner_membership_backfill_preserves_existing_memberships() {
    let migration_dir = unique_temp_path("project-owner-membership-migrations");
    fs::create_dir_all(&migration_dir).expect("temp migration dir creates");
    copy_migrations_up_to(135, &migration_dir);

    let db_path = unique_temp_path("project-owner-membership-db").with_extension("db");
    let url = format!("sqlite://{}", db_path.display());
    let pool = create_sqlite_pool(&url).await.expect("pool");
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("pre-V136 migrations apply");

    let now = "2026-09-10T00:00:00Z";
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES ('owner-v136', 'owner-v136@example.test', 'not-a-password', 'V136 owner', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("owner inserts");

    for (project_id, owner_id) in [
        ("v136-owned", Some("owner-v136")),
        ("v136-existing-member", Some("owner-v136")),
        ("v136-ownerless", None),
        ("v136-dangling-owner", Some("missing-v136-owner")),
    ] {
        sqlx::query(
            "INSERT INTO project (
                id, name, settings, workflow_definition, owner_id, created_at, updated_at
             ) VALUES (?, ?, '{}', '{}', ?, ?, ?)",
        )
        .bind(project_id)
        .bind(project_id)
        .bind(owner_id)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .expect("project inserts");
    }

    sqlx::query(
        "INSERT INTO project_member (
            id, project_id, user_id, role, created_at, updated_at
         ) VALUES ('v136-existing-member-row', 'v136-existing-member', 'owner-v136', 'member', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("existing membership inserts");

    run_migrations(&pool).await.expect("V136 applies");

    let owner_membership: (String, String) = sqlx::query_as(
        "SELECT user_id, role FROM project_member
         WHERE project_id = 'v136-owned'",
    )
    .fetch_one(&pool)
    .await
    .expect("owner membership loads");
    assert_eq!(
        owner_membership,
        ("owner-v136".to_owned(), "owner".to_owned())
    );

    let existing_membership: (String, String) = sqlx::query_as(
        "SELECT user_id, role FROM project_member
         WHERE project_id = 'v136-existing-member'",
    )
    .fetch_one(&pool)
    .await
    .expect("existing membership loads");
    assert_eq!(
        existing_membership,
        ("owner-v136".to_owned(), "member".to_owned())
    );

    let untouched: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_member
         WHERE project_id IN ('v136-ownerless', 'v136-dangling-owner')",
    )
    .fetch_one(&pool)
    .await
    .expect("untouched membership count loads");
    assert_eq!(untouched, 0);

    // The migration is replay-safe and does not add a second owner row.
    run_migrations(&pool)
        .await
        .expect("V136 replay is idempotent");
    let owner_membership_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_member
         WHERE project_id = 'v136-owned' AND user_id = 'owner-v136'",
    )
    .fetch_one(&pool)
    .await
    .expect("owner membership count loads");
    assert_eq!(owner_membership_count, 1);

    pool.close().await;
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
    let retired_task_repo_index: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE name = 'idx_task_repo'")
            .fetch_one(&pool)
            .await
            .expect("retired Task repository index lookup");
    assert_eq!(
        retired_task_repo_index, 0,
        "V138 should remove the Task repository index"
    );
    assert!(
        !task_sql.contains("repo_id"),
        "V138 should remove the Task repository column"
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
