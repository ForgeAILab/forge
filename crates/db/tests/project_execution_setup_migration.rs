use db::{
    create_sqlite_pool, run_migrations, run_migrations_from, AgentRepo, AgentStatus,
    CreateAgentIdentity, CreateAgentProfile, SqliteDb,
};
use serde_json::json;
use sqlx::{Row, SqlitePool};
use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

const NOW: &str = "2026-08-21T00:00:00.000Z";
const OWNER: &str = "v087-owner";

fn migration_dir() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "forge-v087-migrations-{}-{nanos}",
        std::process::id()
    ))
}

fn copy_migrations_up_to(max_version: i64, destination: &Path) {
    fs::create_dir_all(destination).expect("migration directory creates");
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    for entry in fs::read_dir(source).expect("migration directory reads") {
        let entry = entry.expect("migration entry reads");
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(version) = name
            .strip_prefix('V')
            .and_then(|name| name.split_once("__"))
            .and_then(|(version, _)| version.parse::<i64>().ok())
        else {
            continue;
        };
        if version <= max_version {
            fs::copy(&path, destination.join(name)).expect("migration copies");
        }
    }
}

async fn insert_project(pool: &SqlitePool, id: &str, settings: &str) {
    insert_project_with_workflow(pool, id, settings, "{}").await;
}

async fn insert_project_with_workflow(
    pool: &SqlitePool,
    id: &str,
    settings: &str,
    workflow_definition: &str,
) {
    sqlx::query(
        "INSERT INTO project
            (id, name, settings, workflow_definition, owner_id, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(id)
    .bind(format!("V087 {id}"))
    .bind(settings)
    .bind(workflow_definition)
    .bind(OWNER)
    .bind(NOW)
    .bind(NOW)
    .execute(pool)
    .await
    .expect("project inserts");
}

async fn insert_repo(pool: &SqlitePool, project_id: &str, repo_id: &str) {
    sqlx::query(
        "INSERT INTO repo
            (id, project_id, name, remote_url, local_path, work_mode, default_branch,
             created_at, updated_at)
         VALUES (?, ?, 'origin', 'https://example.test/repo.git', NULL, 'direct_merge', 'main', ?, ?)",
    )
    .bind(repo_id)
    .bind(project_id)
    .bind(NOW)
    .bind(NOW)
    .execute(pool)
    .await
    .expect("repo inserts");
    sqlx::query("UPDATE project SET primary_repo_id = ?, updated_at = ? WHERE id = ?")
        .bind(repo_id)
        .bind(NOW)
        .bind(project_id)
        .execute(pool)
        .await
        .expect("primary repo pointer updates");
}

async fn insert_native_agent(pool: &SqlitePool, id: &str, profile_id: &str, archived: bool) {
    insert_native_agent_with_options(
        pool,
        id,
        profile_id,
        AgentStatus::Idle,
        Some(OWNER),
        "account",
        archived,
    )
    .await;
}

async fn insert_native_agent_with_options(
    pool: &SqlitePool,
    id: &str,
    profile_id: &str,
    status: AgentStatus,
    owner_id: Option<&str>,
    visibility: &str,
    archived: bool,
) {
    AgentRepo::create_identity_with_profile(
        &SqliteDb::new(pool.clone()),
        CreateAgentIdentity {
            id: id.to_owned(),
            name: id.to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: owner_id.map(str::to_owned),
            visibility: visibility.to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        CreateAgentProfile {
            id: profile_id.to_owned(),
            identity_id: id.to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("native identity inserts");
    sqlx::query(
        "INSERT INTO agent_connection_health
            (profile_id, status, capability_status_json, checked_at, updated_at)
         VALUES (?, 'healthy', '{\"executor\":\"ready\"}', ?, ?)",
    )
    .bind(profile_id)
    .bind(NOW)
    .bind(NOW)
    .execute(pool)
    .await
    .expect("native health inserts");
    if archived {
        sqlx::query("UPDATE agent_identity SET archived_at = ? WHERE id = ?")
            .bind(NOW)
            .bind(id)
            .execute(pool)
            .await
            .expect("identity archives");
    }
}

async fn insert_unpinned_cli_agent(pool: &SqlitePool, id: &str, profile_id: &str) {
    AgentRepo::create_identity_with_profile(
        &SqliteDb::new(pool.clone()),
        CreateAgentIdentity {
            id: id.to_owned(),
            name: id.to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some(OWNER.to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        CreateAgentProfile {
            id: profile_id.to_owned(),
            identity_id: id.to_owned(),
            backend_kind: "cli".to_owned(),
            executor_type: "shell".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("unpinned CLI identity inserts");
}

async fn insert_online_daemon(pool: &SqlitePool) {
    sqlx::query(
        "INSERT INTO daemon
            (id, machine_id, hostname, os, arch, status, detected_clis_json, created_at, updated_at)
         VALUES ('v087-daemon', 'v087-machine', 'v087-host', 'linux', 'x86_64', 'online',
                 '[{\"kind\":\"shell\",\"availability\":\"authenticated\"}]', ?, ?)",
    )
    .bind(NOW)
    .bind(NOW)
    .execute(pool)
    .await
    .expect("online daemon inserts");
}

async fn operation(pool: &SqlitePool, project_id: &str) -> sqlx::sqlite::SqliteRow {
    sqlx::query(
        "SELECT id, status, current_checkpoint, completed_at, last_error_code
         FROM project_provisioning_operation WHERE project_id = ?",
    )
    .bind(project_id)
    .fetch_one(pool)
    .await
    .expect("operation loads")
}

#[tokio::test]
async fn v087_backfill_is_truthful_and_role_specific() {
    let dir = migration_dir();
    copy_migrations_up_to(86, &dir);
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations_from(&pool, &dir)
        .await
        .expect("pre-V087 migrations apply");

    sqlx::query(
        "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES (?, 'v087@example.test', 'test', 'V087', ?, ?)",
    )
    .bind(OWNER)
    .bind(NOW)
    .bind(NOW)
    .execute(&pool)
    .await
    .expect("owner inserts");

    insert_native_agent(&pool, "worker-ready", "profile-worker-ready", false).await;
    insert_native_agent(&pool, "reviewer-ready", "profile-reviewer-ready", false).await;
    insert_native_agent(&pool, "worker-archived", "profile-worker-archived", true).await;
    insert_native_agent_with_options(
        &pool,
        "worker-busy",
        "profile-worker-busy",
        AgentStatus::Busy,
        Some(OWNER),
        "account",
        false,
    )
    .await;
    insert_native_agent_with_options(
        &pool,
        "reviewer-offline",
        "profile-reviewer-offline",
        AgentStatus::Offline,
        Some(OWNER),
        "account",
        false,
    )
    .await;
    insert_native_agent(&pool, "worker-z", "profile-worker-z", false).await;
    insert_native_agent_with_options(
        &pool,
        "worker-hidden",
        "profile-worker-hidden",
        AgentStatus::Idle,
        Some("other-owner"),
        "account",
        false,
    )
    .await;
    insert_unpinned_cli_agent(&pool, "worker-cli", "profile-worker-cli").await;
    insert_online_daemon(&pool).await;
    insert_project(
        &pool,
        "project-ready",
        &json!({
            "default_role_assignments": [
                {"role_name":"coder", "assignee_type":"agent", "assignee_id":"worker-ready"},
                {"role_name":"reviewer", "assignee_type":"agent", "assignee_id":"reviewer-ready"}
            ]
        })
        .to_string(),
    )
    .await;
    insert_repo(&pool, "project-ready", "repo-ready").await;

    insert_project(
        &pool,
        "project-no-repo",
        &json!({
            "default_role_assignments": [
                {"role_name":"coder", "assignee_type":"agent", "assignee_id":"worker-ready"},
                {"role_name":"reviewer", "assignee_type":"agent", "assignee_id":"reviewer-ready"}
            ]
        })
        .to_string(),
    )
    .await;
    insert_project(
        &pool,
        "project-no-worker",
        &json!({
            "default_role_assignments": [
                {"role_name":"coder", "assignee_type":"agent", "assignee_id":"worker-archived"},
                {"role_name":"reviewer", "assignee_type":"agent", "assignee_id":"reviewer-ready"}
            ]
        })
        .to_string(),
    )
    .await;
    insert_repo(&pool, "project-no-worker", "repo-no-worker").await;
    insert_project(
        &pool,
        "project-self-review",
        &json!({
            "default_role_assignments": [
                {"role_name":"coder", "assignee_type":"agent", "assignee_id":"worker-ready"},
                {"role_name":"reviewer", "assignee_type":"agent", "assignee_id":"worker-ready"}
            ]
        })
        .to_string(),
    )
    .await;
    insert_repo(&pool, "project-self-review", "repo-self-review").await;

    insert_project(
        &pool,
        "project-status-derived-active",
        &json!({
            "default_role_assignments": [
                {"role_name":"coder", "assignee_type":"agent", "assignee_id":"worker-busy"},
                {"role_name":"reviewer", "assignee_type":"agent", "assignee_id":"reviewer-offline"}
            ]
        })
        .to_string(),
    )
    .await;
    insert_repo(
        &pool,
        "project-status-derived-active",
        "repo-status-derived-active",
    )
    .await;

    insert_project(
        &pool,
        "project-unpinned-cli",
        &json!({
            "default_role_assignments": [
                {"role_name":"coder", "assignee_type":"agent", "assignee_id":"worker-cli"},
                {"role_name":"reviewer", "assignee_type":"agent", "assignee_id":"reviewer-ready"}
            ]
        })
        .to_string(),
    )
    .await;
    insert_repo(&pool, "project-unpinned-cli", "repo-unpinned-cli").await;

    let ordered_workflow = json!({
        "roles": [],
        "states": [
            {
                "name": "z_worker_state",
                "kind": "active",
                "column": "In Progress",
                "display_name": "Z Worker",
                "role": "z-worker",
                "hooks": {},
                "gate_config": null,
                "config": {}
            },
            {
                "name": "a_worker_state",
                "kind": "active",
                "column": "In Progress",
                "display_name": "A Worker",
                "role": "a-worker",
                "hooks": {},
                "gate_config": null,
                "config": {}
            },
            {
                "name": "review_state",
                "kind": "gate",
                "column": "Review",
                "display_name": "Review",
                "role": "reviewer",
                "hooks": {},
                "gate_config": null,
                "config": {}
            }
        ]
    });
    insert_project_with_workflow(
        &pool,
        "project-workflow-order",
        &json!({
            "default_role_assignments": [
                {"role_name":"z-worker", "assignee_type":"agent", "assignee_id":"worker-z"},
                {"role_name":"reviewer", "assignee_type":"agent", "assignee_id":"reviewer-ready"}
            ]
        })
        .to_string(),
        &ordered_workflow.to_string(),
    )
    .await;
    insert_repo(&pool, "project-workflow-order", "repo-workflow-order").await;

    let legacy_workflow = json!({
        "roles": [],
        "states": [
            {
                "name": "legacy_build",
                "kind": "active",
                "column": "In Progress",
                "display_name": "Legacy Build",
                "role": "legacy-worker",
                "hooks": {},
                "gate_config": null,
                "config": {}
            },
            {
                "name": "legacy_review",
                "kind": "gate",
                "column": "Review",
                "display_name": "Legacy Review",
                "role": "legacy-reviewer",
                "hooks": {},
                "gate_config": null,
                "config": {}
            }
        ]
    });
    insert_project_with_workflow(
        &pool,
        "project-legacy-workflow",
        &json!({
            "default_role_assignments": [
                {"role_name":"legacy-worker", "assignee_type":"agent", "assignee_id":"worker-ready"},
                {"role_name":"legacy-reviewer", "assignee_type":"agent", "assignee_id":"reviewer-ready"}
            ]
        })
        .to_string(),
        &legacy_workflow.to_string(),
    )
    .await;
    insert_repo(&pool, "project-legacy-workflow", "repo-legacy-workflow").await;

    insert_project(
        &pool,
        "project-duplicate-role",
        &json!({
            "default_role_assignments": [
                {"role_name":"coder", "assignee_type":"agent", "assignee_id":"worker-hidden"},
                {"role_name":"coder", "assignee_type":"agent", "assignee_id":"worker-ready"},
                {"role_name":"reviewer", "assignee_type":"agent", "assignee_id":"worker-ready"}
            ]
        })
        .to_string(),
    )
    .await;
    insert_repo(&pool, "project-duplicate-role", "repo-duplicate-role").await;

    insert_project(
        &pool,
        "project-hidden-latest-role",
        &json!({
            "default_role_assignments": [
                {"role_name":"coder", "assignee_type":"agent", "assignee_id":"worker-ready"},
                {"role_name":"coder", "assignee_type":"agent", "assignee_id":"worker-hidden"},
                {"role_name":"reviewer", "assignee_type":"agent", "assignee_id":"reviewer-ready"}
            ]
        })
        .to_string(),
    )
    .await;
    insert_repo(
        &pool,
        "project-hidden-latest-role",
        "repo-hidden-latest-role",
    )
    .await;

    insert_project(
        &pool,
        "project-duplicate-non-agent",
        &json!({
            "default_role_assignments": [
                {"role_name":"coder", "assignee_type":"agent", "assignee_id":"worker-ready"},
                {"role_name":"coder", "assignee_type":"user", "assignee_id":OWNER},
                {"role_name":"reviewer", "assignee_type":"agent", "assignee_id":"reviewer-ready"}
            ]
        })
        .to_string(),
    )
    .await;
    insert_repo(
        &pool,
        "project-duplicate-non-agent",
        "repo-duplicate-non-agent",
    )
    .await;

    run_migrations(&pool).await.expect("V087 applies");

    for project_id in [
        "project-ready",
        "project-no-repo",
        "project-no-worker",
        "project-self-review",
        "project-status-derived-active",
        "project-unpinned-cli",
        "project-workflow-order",
        "project-legacy-workflow",
        "project-duplicate-role",
        "project-hidden-latest-role",
        "project-duplicate-non-agent",
    ] {
        let row = operation(&pool, project_id).await;
        let id: String = row.try_get("id").expect("operation id");
        assert!(
            db::validate_uuid_v4(&id),
            "operation id must be UUIDv4: {id}"
        );
    }

    let ready = operation(&pool, "project-ready").await;
    assert_eq!(ready.try_get::<String, _>("status").unwrap(), "ready");
    assert_eq!(
        ready.try_get::<String, _>("current_checkpoint").unwrap(),
        "completed"
    );
    assert!(ready
        .try_get::<Option<String>, _>("completed_at")
        .unwrap()
        .is_some());
    assert!(ready
        .try_get::<Option<String>, _>("last_error_code")
        .unwrap()
        .is_none());
    let init = sqlx::query(
        "SELECT c.status AS checkpoint_status,
                c.details_json AS checkpoint_details_json,
                c.completed_at AS checkpoint_completed_at
           FROM project_provisioning_checkpoint c
         JOIN project_provisioning_operation o ON o.id = c.operation_id
         WHERE o.project_id = 'project-ready' AND c.checkpoint = 'repository_initialized'",
    )
    .fetch_one(&pool)
    .await
    .expect("initialization checkpoint");
    assert_eq!(
        init.try_get::<String, _>("checkpoint_status").unwrap(),
        "skipped"
    );
    assert!(init
        .try_get::<Option<String>, _>("checkpoint_completed_at")
        .unwrap()
        .is_some());
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(
            &init
                .try_get::<String, _>("checkpoint_details_json")
                .unwrap()
        )
        .unwrap()["filesystem_verified"],
        false
    );

    for (project_id, code) in [
        ("project-no-repo", "repository_required"),
        ("project-no-worker", "worker_required"),
        ("project-self-review", "independent_reviewer_required"),
    ] {
        let row = operation(&pool, project_id).await;
        assert_eq!(
            row.try_get::<String, _>("status").unwrap(),
            "setup_required"
        );
        assert_eq!(row.try_get::<String, _>("last_error_code").unwrap(), code);
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM project_provisioning_error e
             JOIN project_provisioning_operation o ON o.id = e.operation_id
             WHERE o.project_id = ? AND e.code = ?",
        )
        .bind(project_id)
        .bind(code)
        .fetch_one(&pool)
        .await
        .expect("typed blocker count");
        assert_eq!(count, 1, "expected one {code} blocker for {project_id}");
    }

    for project_id in [
        "project-status-derived-active",
        "project-unpinned-cli",
        "project-workflow-order",
        "project-legacy-workflow",
        "project-duplicate-non-agent",
    ] {
        let row = operation(&pool, project_id).await;
        assert_eq!(row.try_get::<String, _>("status").unwrap(), "ready");
        assert!(row
            .try_get::<Option<String>, _>("completed_at")
            .unwrap()
            .is_some());
    }

    let duplicate = operation(&pool, "project-duplicate-role").await;
    assert_eq!(
        duplicate.try_get::<String, _>("status").unwrap(),
        "setup_required"
    );
    assert_eq!(
        duplicate.try_get::<String, _>("last_error_code").unwrap(),
        "independent_reviewer_required"
    );
    let hidden_latest = operation(&pool, "project-hidden-latest-role").await;
    assert_eq!(
        hidden_latest.try_get::<String, _>("status").unwrap(),
        "setup_required"
    );
    assert_eq!(
        hidden_latest
            .try_get::<String, _>("last_error_code")
            .unwrap(),
        "worker_required"
    );

    let checkpoint_update = sqlx::query(
        "UPDATE project_provisioning_checkpoint
            SET completed_at = NULL
          WHERE operation_id = (SELECT id FROM project_provisioning_operation WHERE project_id = 'project-ready')
            AND checkpoint = 'preflight'",
    )
    .execute(&pool)
    .await;
    assert!(
        checkpoint_update.is_err(),
        "completed checkpoints must retain completed_at"
    );

    fs::remove_dir_all(dir).expect("migration directory cleans");
}
