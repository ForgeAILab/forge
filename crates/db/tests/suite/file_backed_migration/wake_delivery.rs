//! Wake cursors and delivery follow-up migration recovery.

use super::*;

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
