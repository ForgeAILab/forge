//! Execution usage migration provenance and replay.

use super::*;

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
