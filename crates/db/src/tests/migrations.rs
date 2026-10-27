//! Migrations repository tests.
use super::*;

#[tokio::test]
async fn provider_credential_migration_preserves_legacy_handle_defaults() {
    let db = sqlite_db().await;
    let owner = seed_user(&db).await;
    let now = now_rfc3339();
    let id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO credential_handle (
            id, owner_user_id, provider, label, status, created_at, updated_at
         ) VALUES (?, ?, 'openai', 'Legacy key', 'configured', ?, ?)",
    )
    .bind(&id)
    .bind(&owner)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("legacy-shaped handle inserts");
    let handle = CredentialHandleRepo::get_credential_handle(&db, &id)
        .await
        .expect("handle reads")
        .expect("handle exists");
    assert_eq!(handle.credential_method, "api_key");
    assert_eq!(handle.metadata_json, "{}");
    assert!(handle.enabled);
    assert_eq!(handle.version, 1);

    let disabled = CredentialHandleRepo::set_credential_handle_enabled(
        &db,
        &id,
        &owner,
        false,
        handle.version,
        &now_rfc3339(),
    )
    .await
    .expect("provider entry disables");
    assert!(!disabled.enabled);
    assert_eq!(disabled.version, 2);
    let stale = CredentialHandleRepo::set_credential_handle_enabled(
        &db,
        &id,
        &owner,
        true,
        handle.version,
        &now_rfc3339(),
    )
    .await;
    assert!(matches!(stale, Err(DbError::VersionConflict)));
}

#[tokio::test]
async fn migration_creates_schema_and_enforces_foreign_keys() {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");

    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let agent_id = new_uuid_v4();
    let task_id = new_uuid_v4();
    let execution_id = new_uuid_v4();
    let review_id = new_uuid_v4();
    let db = SqliteDb::new(pool.clone());
    let daemon_id = seed_daemon(&db).await;

    assert!(validate_uuid_v4(&project_id));

    sqlx::query(
        "INSERT INTO project (id, name, settings, created_at, updated_at) VALUES (?, ?, '{}', ?, ?)",
    )
    .bind(&project_id)
    .bind("Forge")
    .bind(&now)
    .bind(&now)
    .execute(&pool)
    .await
    .expect("project inserts");

    sqlx::query("INSERT INTO repo (id, project_id, name, remote_url, local_path, work_mode, default_branch, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
    .bind(&repo_id)
    .bind(&project_id)
    .bind("forge")
    .bind("https://example.com/forge.git")
    .bind("/tmp/forge-test-repo")
    .bind("direct_merge")
    .bind("main")
    .bind(&now)
    .bind(&now)
    .execute(&pool)
    .await
    .expect("repo inserts");

    AgentRepo::create(
        &db,
        CreateAgent {
            id: agent_id.clone(),
            name: "shell".to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(daemon_id),
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("agent inserts");

    sqlx::query(
        "INSERT INTO task (id, project_id, title, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(&task_id)
    .bind(&project_id)
    .bind("Build DB foundation")
    .bind(&now)
    .bind(&now)
    .execute(&pool)
    .await
    .expect("task inserts");

    sqlx::query(
        "INSERT INTO execution (id, task_id, agent_id, role, status, logs_path, created_at, updated_at) VALUES (?, ?, ?, 'executor', 'running', ?, ?, ?)",
    )
    .bind(&execution_id)
    .bind(&task_id)
    .bind(&agent_id)
    .bind(format!("sessions/{task_id}/processes/{execution_id}.jsonl"))
    .bind(&now)
    .bind(&now)
    .execute(&pool)
    .await
    .expect("execution inserts");

    sqlx::query(
        "INSERT INTO review (id, task_id, execution_id, attempt_number, status, step_results_json, started_at, created_at, updated_at) VALUES (?, ?, ?, 1, 'running', '[]', ?, ?, ?)",
    )
    .bind(&review_id)
    .bind(&task_id)
    .bind(&execution_id)
    .bind(&now)
    .bind(&now)
    .bind(&now)
    .execute(&pool)
    .await
    .expect("review inserts");

    let bad_execution_result = sqlx::query(
        "INSERT INTO execution (id, task_id, created_at, updated_at) VALUES (?, ?, ?, ?)",
    )
    .bind(new_uuid_v4())
    .bind(new_uuid_v4())
    .bind(&now)
    .bind(&now)
    .execute(&pool)
    .await;
    assert!(bad_execution_result.is_err());

    let bad_review_result = sqlx::query(
        "INSERT INTO review (id, task_id, execution_id, attempt_number, created_at, updated_at) VALUES (?, ?, ?, 1, ?, ?)",
    )
    .bind(new_uuid_v4())
    .bind(&task_id)
    .bind(new_uuid_v4())
    .bind(&now)
    .bind(&now)
    .execute(&pool)
    .await;
    assert!(bad_review_result.is_err());
}

#[tokio::test]
async fn delete_lifecycle_foreign_keys_match_repository_operations() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        None,
        "todo".to_owned(),
        "Delete lifecycle",
    )
    .await;
    let now = now_rfc3339();
    let workspace_id = new_uuid_v4();

    WorkspaceRepo::create(
        &db,
        CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task_id.clone(),
            repo_id: repo_id.clone(),
            worktree_path: "/tmp/forge-delete-lifecycle".to_owned(),
            branch: workspace::task_branch_name(&task_id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");

    let execution = ExecutionRepo::create(
        &db,
        CreateExecution {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "executor".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace_id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("execution creates");

    sqlx::query(
        "INSERT INTO transition_log (id, task_id, from_state, to_state, trigger_name, triggered_by, trigger_reason, created_at) VALUES (?, ?, 'todo', 'in_progress', NULL, 'system', 'test', ?)",
    )
    .bind(new_uuid_v4())
    .bind(&task_id)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("transition log creates");

    WorkspaceRepo::delete(&db, &workspace_id)
        .await
        .expect("workspace delete clears execution link");
    // Project deletion refuses while an Execution is still running, which is
    // a separate guard from the cascade this case covers: settle the
    // execution first so the cascade itself is what gets exercised.
    sqlx::query("UPDATE execution SET status = 'completed', stopped_at = ? WHERE id = ?")
        .bind(now_rfc3339())
        .bind(&execution.id)
        .execute(db.pool())
        .await
        .expect("running execution settles before the project is deleted");
    let execution = ExecutionRepo::get_by_id(&db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution remains");
    assert_eq!(execution.workspace_id, None);

    ProjectRepo::delete(&db, &project_id)
        .await
        .expect("project delete cascades through task data");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task WHERE project_id = ?")
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .expect("task count"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM transition_log WHERE task_id = ?")
            .bind(&task_id)
            .fetch_one(db.pool())
            .await
            .expect("transition count"),
        0
    );

    let (project_id, repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(&db, &project_id, None, "todo".to_owned(), "Repo cascade").await;
    RepoRepo::delete(&db, &repo_id)
        .await
        .expect("repo delete succeeds without deleting task history");
    let task = TaskRepo::get_by_id(&db, &task_id, true)
        .await
        .expect("task lookup succeeds")
        .expect("task remains after repository deletion");
    assert_eq!(task.project_id, project_id);
    let project = ProjectRepo::get_by_id(&db, &project_id)
        .await
        .expect("project lookup succeeds")
        .expect("project remains after repository deletion");
    assert_eq!(project.primary_repo_id, None);
}

#[tokio::test]
async fn migration_runner_is_idempotent() {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");

    run_migrations(&pool).await.expect("first run succeeds");
    run_migrations(&pool).await.expect("second run succeeds");

    let applied_count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _migration")
        .fetch_one(&pool)
        .await
        .expect("migration count loads");
    let expected_count = std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"))
        .expect("migration directory reads")
        .filter(|entry| {
            entry
                .as_ref()
                .ok()
                .and_then(|entry| entry.path().file_name()?.to_str().map(str::to_owned))
                .is_some_and(|filename| filename.starts_with('V') && filename.ends_with(".sql"))
        })
        .count() as i64;
    assert_eq!(applied_count, expected_count);
}

#[tokio::test]
async fn remove_knowledge_lifecycle_hooks_migration_cleans_only_retired_plugins() {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");

    let changed = r#"{ "max_active_tasks" : 7, "lifecycle_hooks": {"before_work": [ {"type":"script","command":"make setup"}, {"type":"plugin","name":"knowledge-inject","enabled":true}, {"type":"plugin","name":"other","config":{"keep":1}}, {"type":"plugin","name":"knowledge-capture"} ], "on_task_done": [{"type":"plugin","name":"knowledge-capture"},{"type":"script","command":"finish"}]}, "tail" : [1.00, "\u0061"] }"#;
    let unchanged = r#" { "lifecycle_hooks" : { "before_work" : [ { "type" : "plugin", "name" : "other" } ] }, "untouched" : true } "#;
    let invalid = "{not json";
    for (id, settings) in [
        ("changed", changed),
        ("unchanged", unchanged),
        ("invalid", invalid),
    ] {
        sqlx::query(
            "INSERT INTO project (id, name, settings, created_at, updated_at)
             VALUES (?, ?, ?, '2026-10-01T21:20:00Z', '2026-10-01T21:20:00Z')",
        )
        .bind(id)
        .bind(id)
        .bind(settings)
        .execute(&pool)
        .await
        .expect("legacy Project inserts");
    }

    sqlx::query("DELETE FROM _migration WHERE version = 202610012120")
        .execute(&pool)
        .await
        .expect("knowledge hook migration marker clears");
    run_migrations(&pool)
        .await
        .expect("knowledge hook migration re-applies");

    let load_settings = |id: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, String>("SELECT settings FROM project WHERE id = ?")
                .bind(id)
                .fetch_one(&pool)
                .await
                .expect("Project settings load")
        }
    };

    assert_eq!(
        load_settings("changed").await,
        r#"{"max_active_tasks":7,"lifecycle_hooks":{"before_work":[{"type":"script","command":"make setup"},{"type":"plugin","name":"other","config":{"keep":1}}],"on_task_done":[{"type":"script","command":"finish"}]},"tail":[1.00,"\u0061"]}"#
    );
    assert_eq!(load_settings("unchanged").await, unchanged);
    assert_eq!(load_settings("invalid").await, invalid);
}

#[tokio::test]
async fn execution_liveness_migration_preserves_history_and_does_not_fabricate_owner() {
    let migration_root =
        std::env::temp_dir().join(format!("forge-db-migrations-{}", new_uuid_v4()));
    std::fs::create_dir_all(&migration_root).expect("migration temp directory creates");
    let source_root = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"));
    for entry in std::fs::read_dir(source_root)
        .expect("migration source directory reads")
        .flatten()
    {
        let path = entry.path();
        let Some(filename) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if filename == "V089__execution_liveness.sql" {
            continue;
        }
        std::fs::copy(&path, migration_root.join(filename)).expect("migration copies");
    }

    let database_path =
        std::env::temp_dir().join(format!("forge-execution-liveness-{}.sqlite", new_uuid_v4()));
    let database_url = format!("sqlite://{}", database_path.display());
    let pool = create_sqlite_pool(&database_url)
        .await
        .expect("pool creates");
    run_migrations_from(&pool, &migration_root)
        .await
        .expect("pre-liveness migrations run");
    let now = "2026-08-21T00:00:00Z";
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let task_id = new_uuid_v4();
    let terminal_id = new_uuid_v4();
    let running_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO project (id, name, settings, created_at, updated_at)
         VALUES (?, 'migration-test', '{}', ?, ?)",
    )
    .bind(&project_id)
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("project inserts");
    sqlx::query(
        "INSERT INTO repo (id, project_id, name, remote_url, local_path, work_mode, default_branch, created_at, updated_at)
         VALUES (?, ?, 'migration-repo', '/tmp/migration-repo', '/tmp/migration-repo', 'direct_merge', 'main', ?, ?)",
    )
    .bind(&repo_id)
    .bind(&project_id)
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("repo inserts");
    sqlx::query(
        "INSERT INTO task (id, project_id, title, status, created_at, updated_at)
         VALUES (?, ?, 'migration task', 'done', ?, ?)",
    )
    .bind(&task_id)
    .bind(&project_id)
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("task inserts");
    sqlx::query(
        "INSERT INTO execution (id, task_id, role, status, stopped_at, summary, logs_path, error, created_at, updated_at)
         VALUES (?, ?, 'executor', 'completed', '2026-08-21T00:00:01Z', 'historical summary',
                 'logs/historical.jsonl', 'historical error', ?, ?)",
    )
    .bind(&terminal_id)
    .bind(&task_id)
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("terminal execution inserts");
    sqlx::query(
        "INSERT INTO execution (id, task_id, role, status, summary, logs_path, created_at, updated_at)
         VALUES (?, ?, 'executor', 'running', 'running summary', 'logs/running.jsonl', ?, ?)",
    )
    .bind(&running_id)
    .bind(&task_id)
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .expect("running execution inserts");

    let liveness_path = source_root.join("V089__execution_liveness.sql");
    std::fs::copy(
        &liveness_path,
        migration_root.join("V089__execution_liveness.sql"),
    )
    .expect("liveness migration copies");
    run_migrations_from(&pool, &migration_root)
        .await
        .expect("liveness migration runs");

    let terminal = sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
        ),
    >(
        "SELECT status, summary, logs_path, error, lease_owner, hard_deadline_at
         FROM execution WHERE id = ?",
    )
    .bind(&terminal_id)
    .fetch_one(&pool)
    .await
    .expect("terminal execution loads");
    assert_eq!(terminal.0, "completed");
    assert_eq!(terminal.1, "historical summary");
    assert_eq!(terminal.2, "logs/historical.jsonl");
    assert_eq!(terminal.3.as_deref(), Some("historical error"));
    assert!(terminal.4.is_none());
    assert!(terminal.5.is_none());

    let running =
        sqlx::query_as::<_, (String, Option<String>, Option<String>, Option<String>, i64)>(
            "SELECT updated_at, lease_owner, last_heartbeat_at, hard_deadline_at, execution_version
         FROM execution WHERE id = ?",
        )
        .bind(&running_id)
        .fetch_one(&pool)
        .await
        .expect("running execution loads");
    assert_eq!(running.1, None);
    assert_eq!(running.2, None);
    assert_eq!(running.3.as_deref(), Some(now));
    assert_eq!(running.4, 1);

    drop(pool);
    let reopened = create_sqlite_pool(&database_url)
        .await
        .expect("file-backed database reopens");
    let reopened_execution: (String, Option<String>, Option<String>, Option<String>) =
        sqlx::query_as(
            "SELECT status, lease_owner, last_heartbeat_at, hard_deadline_at
             FROM execution WHERE id = ?",
        )
        .bind(&running_id)
        .fetch_one(&reopened)
        .await
        .expect("reopened running execution loads");
    assert_eq!(reopened_execution.0, "running");
    assert!(reopened_execution.1.is_none());
    assert!(reopened_execution.2.is_none());
    assert_eq!(reopened_execution.3.as_deref(), Some(now));
    drop(reopened);
    std::fs::remove_file(&database_path).expect("database file removes");
    std::fs::remove_file(format!("{}-wal", database_path.display())).ok();
    std::fs::remove_file(format!("{}-shm", database_path.display())).ok();
    std::fs::remove_dir_all(&migration_root).expect("migration temp directory removes");
}

#[tokio::test]
async fn test_normalize_failure_kinds_migration_backfill() {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");

    // Seed legacy rows below the FK layer, then re-apply V056 over them.
    let mut conn = pool.acquire().await.expect("connection acquires");
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *conn)
        .await
        .expect("fk off");
    let now = now_rfc3339();
    let insert = |id: &str,
                  error_annotation: Option<&str>,
                  blocked: Option<&str>,
                  failed: Option<&str>| {
        sqlx::query(
            "INSERT INTO task (id, project_id, title, status, error_annotation, blocked_json, failed_json, created_at, updated_at)
             VALUES (?, 'p', 'legacy', 'blocked', ?, ?, ?, ?, ?)",
        )
        .bind(id.to_owned())
        .bind(error_annotation.map(str::to_owned))
        .bind(blocked.map(str::to_owned))
        .bind(failed.map(str::to_owned))
        .bind(now.clone())
        .bind(now.clone())
    };
    insert(
        "t-alias",
        Some(r#"{"type":"retry_budget_exhausted","blocking_reason":"x"}"#),
        None,
        None,
    )
    .execute(&mut *conn)
    .await
    .expect("alias row inserts");
    insert(
        "t-prose",
        None,
        Some(r#"{"reason":"review retry budget exhausted after 3 attempts","created_at":"2026-01-01T00:00:00Z"}"#),
        None,
    )
    .execute(&mut *conn)
    .await
    .expect("prose row inserts");
    insert(
        "t-crash",
        Some(r#"{"type":"crash","message":"boom"}"#),
        None,
        Some(r#"{"reason":"crashed","created_at":"2026-01-01T00:00:00Z","kind":"crash"}"#),
    )
    .execute(&mut *conn)
    .await
    .expect("crash row inserts");
    insert(
        "t-malformed",
        Some("{not json"),
        Some("also not json"),
        None,
    )
    .execute(&mut *conn)
    .await
    .expect("malformed row inserts");
    insert(
        "t-unknown",
        None,
        Some(r#"{"reason":"strange","created_at":"2026-01-01T00:00:00Z","kind":"从未见过"}"#),
        None,
    )
    .execute(&mut *conn)
    .await
    .expect("unknown row inserts");
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut *conn)
        .await
        .expect("fk on");
    drop(conn);

    sqlx::query("DELETE FROM _migration WHERE version = 56")
        .execute(&pool)
        .await
        .expect("migration marker clears");
    run_migrations(&pool).await.expect("migration re-applies");

    let field = |id: &str, column: &str| {
        let sql = format!("SELECT {column} FROM task WHERE id = ?");
        let pool = pool.clone();
        let id = id.to_owned();
        async move {
            sqlx::query_scalar::<_, Option<String>>(&sql)
                .bind(id)
                .fetch_one(&pool)
                .await
                .expect("row fetches")
        }
    };

    // Alias type renamed.
    let annotation = field("t-alias", "error_annotation").await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&annotation).unwrap()["type"],
        "retry_exhausted"
    );
    // Prose-only row gains the structured kind; other fields untouched.
    let blocked = field("t-prose", "blocked_json").await.unwrap();
    let blocked: serde_json::Value = serde_json::from_str(&blocked).unwrap();
    assert_eq!(blocked["kind"], "retry_exhausted");
    assert_eq!(
        blocked["reason"],
        "review retry budget exhausted after 3 attempts"
    );
    // crash maps to executor_failed in both carriers.
    let annotation = field("t-crash", "error_annotation").await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&annotation).unwrap()["type"],
        "executor_failed"
    );
    let failed = field("t-crash", "failed_json").await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&failed).unwrap()["kind"],
        "executor_failed"
    );
    // Malformed JSON is untouched.
    assert_eq!(
        field("t-malformed", "error_annotation").await.as_deref(),
        Some("{not json")
    );
    assert_eq!(
        field("t-malformed", "blocked_json").await.as_deref(),
        Some("also not json")
    );
    // Unmappable kinds are untouched (they deserialize to Unknown at read time).
    let blocked = field("t-unknown", "blocked_json").await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&blocked).unwrap()["kind"],
        "从未见过"
    );
}
