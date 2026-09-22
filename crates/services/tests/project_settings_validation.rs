//! The settings rules REST and MCP now share.
//!
//! Each surface had its own copy and they had drifted in both directions, so
//! these cases pin the union: the membership checks MCP was missing, and the
//! hook-timeout check REST was missing.

use std::sync::Arc;

use api_types::WorkflowDefinition;
use db::{create_sqlite_pool, now_rfc3339, run_migrations, CreateProject, ProjectRepo, SqliteDb};
use serde_json::{json, Value};
use services::project_settings::validate_project_settings;

const PROJECT: &str = "settings-validation-project";
const MEMBER: &str = "settings-validation-member";
const OUTSIDER: &str = "settings-validation-outsider";

async fn fixture() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let now = now_rfc3339();
    for (id, email) in [
        (MEMBER, "member@example.test"),
        (OUTSIDER, "outsider@example.test"),
    ] {
        sqlx::query(
            "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
             VALUES (?, ?, 'test', NULL, ?, ?)",
        )
        .bind(id)
        .bind(email)
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("user creates");
    }
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: PROJECT.to_owned(),
            name: "Settings validation".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(MEMBER.to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("Project creates");
    db
}

fn workflow() -> WorkflowDefinition {
    services::workflow::default_workflow::default_workflow()
}

fn assignment(role: &str, assignee_type: &str, assignee_id: Option<&str>) -> Value {
    json!({
        "default_role_assignments": [{
            "role_name": role,
            "assignee_type": assignee_type,
            "assignee_id": assignee_id,
        }]
    })
}

#[tokio::test]
async fn a_user_assignee_must_belong_to_the_project() {
    let db = fixture().await;
    // The check MCP did not have: an id that names nobody in this Project.
    let error = validate_project_settings(
        &db,
        &assignment("coder", "user", Some(OUTSIDER)),
        &workflow(),
        Some(PROJECT),
        None,
    )
    .await
    .expect_err("a non-member must not be a default assignee");
    assert!(error.to_string().contains("project member"), "{error}");
}

#[tokio::test]
async fn the_legacy_human_assignee_is_not_checked_for_membership() {
    let db = fixture().await;
    // Projects created before membership records carry this sentinel; it
    // names no account, so there is nothing to look up.
    validate_project_settings(
        &db,
        &assignment("coder", "user", Some("human")),
        &workflow(),
        Some(PROJECT),
        None,
    )
    .await
    .expect("the legacy manual assignee stays valid");
}

#[tokio::test]
async fn a_script_hook_must_declare_a_usable_timeout() {
    let db = fixture().await;
    // The check REST did not have.
    let settings = json!({
        "lifecycle_hooks": {
            "before_work": [{
                "type": "script",
                "command": "echo hi",
                "timeout_seconds": 0,
                "blocking": true,
            }]
        }
    });
    let error = validate_project_settings(&db, &settings, &workflow(), Some(PROJECT), None)
        .await
        .expect_err("a zero timeout is not a timeout");
    assert!(error.to_string().contains("timeout_seconds"), "{error}");
}

#[tokio::test]
async fn a_blocking_hook_outside_before_work_is_refused() {
    let db = fixture().await;
    let settings = json!({
        "lifecycle_hooks": {
            "after_work": [{
                "type": "script",
                "command": "echo hi",
                "timeout_seconds": 30,
                "blocking": true,
            }]
        }
    });
    let error = validate_project_settings(&db, &settings, &workflow(), Some(PROJECT), None)
        .await
        .expect_err("only before_work may block");
    assert!(error.to_string().contains("before_work"), "{error}");
}

#[tokio::test]
async fn structural_rules_apply_even_without_a_project_to_check_against() {
    let db = fixture().await;
    for (settings, expected) in [
        (
            assignment("not-a-role", "user", Some(MEMBER)),
            "unknown role",
        ),
        (assignment("coder", "user", None), "requires assignee_id"),
        (
            assignment("coder", "user", Some("   ")),
            "requires assignee_id",
        ),
        (assignment("coder", "robot", Some(MEMBER)), "assignee_type"),
        (
            json!({"retry_budgets": {"review": -1}}),
            "retry_budgets.review",
        ),
    ] {
        let error = validate_project_settings(&db, &settings, &workflow(), None, None)
            .await
            .expect_err("a structural rule does not need a Project to apply");
        assert!(
            error.to_string().contains(expected),
            "refusal must name {expected}: {error}"
        );
    }
}
