//! How the effective workspace command allowlist is resolved.
//!
//! Forge ships a built-in set covering build and test tooling. An owner
//! widens or replaces it in the Forge config, and a Project layers its own
//! settings over that result — so a Rust monorepo and a Python service under
//! the same server do not have to share one list, and neither can a model
//! choose its own.

use std::sync::Arc;

use config::CommandPolicyConfig;
use db::{create_sqlite_pool, now_rfc3339, run_migrations, CreateProject, ProjectRepo, SqliteDb};
use services::EmbeddedAgentService;

const PROJECT_ID: &str = "command-allowlist-project";

async fn service_with_project(settings: &str) -> (Arc<SqliteDb>, EmbeddedAgentService) {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES ('allowlist-user', 'allowlist@example.test', 'test', NULL, ?, ?)",
    )
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("user creates");
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: PROJECT_ID.to_owned(),
            name: "Allowlist Project".to_owned(),
            settings: settings.to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some("allowlist-user".to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("Project creates");
    let service = EmbeddedAgentService::new(Arc::clone(&db), b"allowlist-test-key");
    (db, service)
}

#[tokio::test]
async fn an_unconfigured_server_hands_every_workspace_the_builtin_set() {
    let (_db, service) = service_with_project("{}").await;
    let allowlist = service.effective_command_allowlist(None).await;
    assert!(allowlist.allows("cargo") && allowlist.allows("pnpm") && allowlist.allows("git"));
    assert!(
        !allowlist.allows("docker"),
        "escape hatches stay out until an owner adds them"
    );
}

#[tokio::test]
async fn the_config_baseline_applies_to_every_scope_including_one_without_a_project() {
    let (_db, service) = service_with_project("{}").await;
    service.set_command_policy(&CommandPolicyConfig {
        allow: vec!["docker".to_owned()],
        only: None,
    });

    for scope in [None, Some(PROJECT_ID)] {
        let allowlist = service.effective_command_allowlist(scope).await;
        assert!(
            allowlist.allows("docker"),
            "configured program, scope {scope:?}"
        );
        assert!(
            allowlist.allows("cargo"),
            "built-in program, scope {scope:?}"
        );
    }
}

#[tokio::test]
async fn a_project_layers_its_own_settings_over_the_configured_baseline() {
    let (_db, service) = service_with_project(
        &serde_json::json!({
            "command_allowlist": { "allow": ["terraform"] }
        })
        .to_string(),
    )
    .await;
    service.set_command_policy(&CommandPolicyConfig {
        allow: vec!["docker".to_owned()],
        only: None,
    });

    let project = service.effective_command_allowlist(Some(PROJECT_ID)).await;
    assert!(project.allows("terraform"), "the Project's own addition");
    assert!(project.allows("docker"), "and the configured baseline");
    assert!(project.allows("cargo"), "and the built-in set");

    let account = service.effective_command_allowlist(None).await;
    assert!(
        !account.allows("terraform"),
        "a Project's addition must not leak into another scope"
    );
}

#[tokio::test]
async fn a_project_that_pins_only_drops_everything_it_does_not_name() {
    let (_db, service) = service_with_project(
        &serde_json::json!({
            "command_allowlist": { "only": ["python3", "pytest"], "allow": ["uv"] }
        })
        .to_string(),
    )
    .await;
    service.set_command_policy(&CommandPolicyConfig {
        allow: vec!["docker".to_owned()],
        only: None,
    });

    let project = service.effective_command_allowlist(Some(PROJECT_ID)).await;
    assert!(project.allows("python3") && project.allows("pytest") && project.allows("uv"));
    assert!(
        !project.allows("cargo"),
        "`only` replaces the inherited set"
    );
    assert!(
        !project.allows("docker"),
        "including the server's own additions"
    );
    assert_eq!(project.len(), 3);
}

#[tokio::test]
async fn a_malformed_project_entry_narrows_nothing_and_never_widens_the_set() {
    let (_db, service) = service_with_project(
        &serde_json::json!({
            "command_allowlist": { "allow": ["../../bin/sh", "/bin/sh", "sh -c", 7] }
        })
        .to_string(),
    )
    .await;

    let project = service.effective_command_allowlist(Some(PROJECT_ID)).await;
    assert_eq!(
        project.len(),
        services::EmbeddedAgentService::new(
            Arc::new(SqliteDb::new(
                create_sqlite_pool("sqlite::memory:").await.expect("pool")
            )),
            b"unused"
        )
        .effective_command_allowlist(None)
        .await
        .len(),
        "a Project full of unusable entries gets exactly the baseline"
    );
    assert!(project.allows("cargo"));
    assert!(!project.allows("sh -c"));
}
