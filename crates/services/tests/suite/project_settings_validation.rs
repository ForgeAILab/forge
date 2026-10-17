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

#[tokio::test]
async fn the_project_environment_is_validated() {
    let db = fixture().await;
    let valid = json!({
        "environment": {
            "env": { "GODOT_BIN": "/opt/godot/4.7.2/godot" },
            "assets": [{ "source": "/srv/limezu", "target": "assets/vendor/limezu" }],
            "checks": [{ "name": "godot", "command": "\"$GODOT_BIN\" --version", "roles": ["reviewer"] }],
        }
    });
    validate_project_settings(&db, &valid, &workflow(), Some(PROJECT), None)
        .await
        .expect("a well-formed environment is accepted");

    for (environment, expected) in [
        (json!({ "env": { "FORGE_TASK_ID": "x" } }), "reserved"),
        (
            json!({ "assets": [{ "source": "/srv/art", "target": "../escape" }] }),
            "inside the worktree",
        ),
        (
            json!({ "checks": [{ "name": "slow", "command": "true", "timeout_seconds": 0 }] }),
            "between 1 and 300",
        ),
        (
            json!({ "checks": [{ "name": "too-slow", "command": "true", "timeout_seconds": 301 }] }),
            "between 1 and 300",
        ),
        (
            json!({ "checks": [{ "name": "browser", "command": "true", "roles": ["reviewre"] }] }),
            "unknown role",
        ),
        (
            json!({
                "assets": [
                    { "source": "/srv/art", "target": "vendor" },
                    { "source": "/srv/models", "target": "vendor/models" }
                ]
            }),
            "overlap",
        ),
        (json!({ "enb": { "TOOL": "path" } }), "unknown field"),
    ] {
        let error = validate_project_settings(
            &db,
            &json!({ "environment": environment }),
            &workflow(),
            Some(PROJECT),
            None,
        )
        .await
        .expect_err("an unsafe environment is refused");
        assert!(error.to_string().contains(expected), "{expected}: {error}");
    }
}

#[tokio::test]
async fn review_round_routing_progress_setting_is_validated_and_resolved() {
    let db = fixture().await;
    for limit in [0, 2, 4] {
        let settings = json!({"retry_budgets":{"review_progress":limit}});
        validate_project_settings(&db, &settings, &workflow(), Some(PROJECT), None)
            .await
            .unwrap();
        let state = serde_json::from_value(json!({
            "name":"review", "kind":"gate", "column":"Review",
            "display_name":"Review", "hooks":{}, "config":{}
        }))
        .unwrap();
        let config = db::budget::state_config(&state, &settings.to_string());
        assert_eq!(config["retry_budgets"]["review_progress"], limit);
    }
    let error = validate_project_settings(
        &db,
        &json!({"retry_budgets":{"review_progress":-1}}),
        &workflow(),
        Some(PROJECT),
        None,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("retry_budgets.review_progress"));
}

#[tokio::test]
async fn executions_repair_model_switch_setting_is_opt_in_and_validated() {
    let db = fixture().await;
    assert!(!api_types::ProjectSettings::default().repair_switch_model_when_stuck);
    for enabled in [false, true] {
        let settings = json!({"repair_switch_model_when_stuck":enabled});
        validate_project_settings(&db, &settings, &workflow(), Some(PROJECT), None)
            .await
            .unwrap();
        let parsed: api_types::ProjectSettings = serde_json::from_value(settings).unwrap();
        assert_eq!(parsed.repair_switch_model_when_stuck, enabled);
    }
    for value in [json!(null), json!("true"), json!(1), json!({})] {
        assert!(validate_project_settings(
            &db,
            &json!({"repair_switch_model_when_stuck":value}),
            &workflow(),
            Some(PROJECT),
            None
        )
        .await
        .is_err());
    }
}

#[tokio::test]
async fn review_verify_project_settings_reject_invalid_fields() {
    let db = fixture().await;
    let settings = json!({"default_review_config":{"verify":{"ready_path":"/","ready_command":"true","checks":[{"name":"smoke","command":"true"}]}}});
    let error = validate_project_settings(&db, &settings, &workflow(), Some(PROJECT), Some(MEMBER))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("verify.ready_command"));
    let valid =
        json!({"default_review_config":{"verify":{"checks":[{"name":"smoke","command":"true"}]}}});
    validate_project_settings(&db, &valid, &workflow(), Some(PROJECT), Some(MEMBER))
        .await
        .unwrap();
}

async fn recorded_review_prompt(verify: bool) -> String {
    let db = fixture().await;
    let now = now_rfc3339();
    let task = db::TaskRepo::create(
        &*db,
        db::CreateTask {
            id: db::new_uuid_v4(),
            project_id: PROJECT.into(),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "Save draft".into(),
            description: None,
            task_type: "task".into(),
            status: "review".into(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: Some(json!({"review":{"ci_steps":["cargo test"]}}).to_string()),
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let candidate = db::new_uuid_v4();
    sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES(?,?,'coder','completed',?,?)").bind(&candidate).bind(&task.id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    let mut details = json!({"ci_steps":[{"index":0,"command":"cargo test","exit_code":0,"stderr_tail":"","output_tail":"test save passed"}]});
    if verify {
        details["verify"] = json!({"status":"pass","checks":[{"name":"smoke","result":"pass","summary":"Save persisted","evidence":["media-one"],"evidence_paths":["/task/.forge-task/verify/1/screen-one.png"]}]});
    }
    db::ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: db::new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate,
            attempt_number: 1,
            status: db::ReviewStatus::Running,
            step_results_json: details.to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let router = services::lifecycle::context::embedded_workspace_router_for_test(
        db.clone(),
        temp.path().to_owned(),
        None,
    );
    let context = services::workflow::dispatch::loader::load_agent_dispatch_context(
        services::workflow::dispatch::loader::DispatchContextParams {
            db,
            router: &router,
            task_id: &task.id,
            role: "reviewer",
            state_name: "review",
            state_config: json!({"review":{"ci_steps":["cargo test"]}}),
            execution_policy: None,
            workflow: &workflow(),
        },
    )
    .await
    .unwrap();
    use services::workflow::dispatch::PromptBuilder;
    services::workflow::dispatch::reviewer_prompt::ReviewerPromptBuilder
        .build(&context)
        .user
}
#[tokio::test]
async fn reviewer_receives_and_can_cite_the_recorded_pre_review_ci_result() {
    let prompt = recorded_review_prompt(false).await;
    assert!(prompt.contains("Forge already ran these checks"));
    assert!(prompt.contains("cargo test"));
    assert!(prompt.contains("test save passed"));
}
#[tokio::test]
async fn reviewer_receives_and_can_cite_the_recorded_pre_review_verify_result() {
    let prompt = recorded_review_prompt(true).await;
    assert!(prompt.contains("Verify results:"));
    assert!(prompt.contains("Save persisted"));
    assert!(prompt.contains("screen-one.png"));
    assert!(prompt.contains("media-one"));
}
