use api_types::{StateKind, WorkflowDefinition, WorkflowTrigger};
use db::{create_sqlite_pool, run_migrations, SqliteDb, TaskRepo};
use services::workflow::default_workflow::default_workflow;

const DEFAULT_WORKFLOW_FIXTURE: &str = include_str!("fixtures/default_strict_workflow.json");
const CUSTOM_WORKFLOW_FIXTURE: &str = include_str!("fixtures/legacy_custom_workflow.json");
const LIFECYCLE_TASKS_FIXTURE: &str = include_str!("fixtures/legacy_lifecycle_tasks.sql");

fn load_workflow(fixture_name: &str, fixture: &str) -> WorkflowDefinition {
    serde_json::from_str(fixture).unwrap_or_else(|error| {
        panic!("{fixture_name} must deserialize as WorkflowDefinition: {error}")
    })
}

#[test]
fn legacy_workflow_fixtures_round_trip_through_current_types() {
    for (fixture_name, fixture) in [
        ("default strict workflow", DEFAULT_WORKFLOW_FIXTURE),
        ("custom workflow", CUSTOM_WORKFLOW_FIXTURE),
    ] {
        let workflow = load_workflow(fixture_name, fixture);
        let round_tripped: WorkflowDefinition =
            serde_json::from_str(&serde_json::to_string(&workflow).expect("workflow serializes"))
                .unwrap_or_else(|error| panic!("{fixture_name} round-trips: {error}"));
        assert_eq!(
            workflow, round_tripped,
            "{fixture_name} changed on round-trip"
        );
    }
}

#[test]
fn default_workflow_matches_checked_in_strict_fixture() {
    let fixture = load_workflow("default strict workflow", DEFAULT_WORKFLOW_FIXTURE);
    assert_eq!(
        default_workflow(),
        fixture,
        "current default workflow changed; update the fixture only with an intentional compatibility decision"
    );
}

#[test]
fn custom_fixture_is_a_valid_renamed_legacy_workflow() {
    let workflow = load_workflow("custom workflow", CUSTOM_WORKFLOW_FIXTURE);

    assert!(workflow.roles.iter().any(|role| role.name == "implementer"));
    assert!(workflow
        .states
        .iter()
        .any(|state| { state.name == "waiting_on_customer" && state.kind == StateKind::Custom }));
    assert!(workflow
        .states
        .iter()
        .any(|state| { state.name == "building" && state.role.as_deref() == Some("implementer") }));
}

#[tokio::test]
async fn lifecycle_database_fixture_covers_current_legacy_state_graph() {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("sqlite pool creates");
    run_migrations(&pool).await.expect("migrations run");
    sqlx::raw_sql(LIFECYCLE_TASKS_FIXTURE)
        .execute(&pool)
        .await
        .expect("lifecycle fixture seeds");

    let db = SqliteDb::new(pool);
    let workflow = default_workflow();
    let expected_statuses = [
        ("fixture-task-backlog", "backlog", StateKind::Backlog),
        ("fixture-task-planning", "planning", StateKind::Gate),
        ("fixture-task-active", "in_progress", StateKind::Active),
        ("fixture-task-review", "review", StateKind::Gate),
        (
            "fixture-task-merge-failed",
            "merge_failed",
            StateKind::Active,
        ),
        ("fixture-task-done", "done", StateKind::Terminal),
        ("fixture-task-cancelled", "cancelled", StateKind::Terminal),
    ];

    for (task_id, expected_status, expected_kind) in expected_statuses {
        let task = TaskRepo::get_by_id(&db, task_id, false)
            .await
            .expect("task loads")
            .unwrap_or_else(|| panic!("fixture task {task_id} is present"));
        assert_eq!(task.status, expected_status, "fixture status for {task_id}");
        assert_eq!(
            workflow.state_kind(&task.status),
            Some(expected_kind),
            "fixture status {expected_status} is part of the current workflow"
        );
    }

    let expected_transitions = [
        (
            "backlog",
            &[
                (WorkflowTrigger::Accept, "todo"),
                (WorkflowTrigger::Reject, "cancelled"),
            ][..],
        ),
        (
            "planning",
            &[
                (WorkflowTrigger::Accept, "in_progress"),
                (WorkflowTrigger::Reject, "planning"),
            ][..],
        ),
        ("in_progress", &[(WorkflowTrigger::Accept, "review")][..]),
        (
            "review",
            &[
                (WorkflowTrigger::Accept, "merging"),
                (WorkflowTrigger::Reject, "in_progress"),
            ][..],
        ),
        (
            "merge_failed",
            &[
                (WorkflowTrigger::Accept, "review"),
                (WorkflowTrigger::Fail, "cancelled"),
                (WorkflowTrigger::Retry, "in_progress"),
            ][..],
        ),
        ("done", &[][..]),
        ("cancelled", &[][..]),
    ];

    for (state, expected) in expected_transitions {
        let actual: Vec<_> = workflow.outgoing_trigger_targets(state).collect();
        let expected: Vec<_> = expected
            .iter()
            .map(|(trigger, target)| (*trigger, (*target).to_owned()))
            .collect();
        assert_eq!(actual, expected, "legacy transitions from {state} changed");
    }
}

/// V148 puts `carry_review_authority` at the front of every stored `review`
/// `on_enter` list. Replaying its UPDATE over the checked-in strict workflow
/// with the hook stripped must reproduce the current default exactly, and a
/// second run must change nothing.
#[tokio::test]
async fn v148_inserts_the_carry_hook_before_reviewer_dispatch_idempotently() {
    const V148: &str = include_str!("../../db/migrations/V148__review_authority_carry.sql");
    let update_sql = &V148[V148
        .find("UPDATE project")
        .expect("V148 carries the workflow UPDATE")..];

    let current: serde_json::Value =
        serde_json::from_str(DEFAULT_WORKFLOW_FIXTURE).expect("fixture parses");
    let mut legacy = current.clone();
    let review_index = legacy["states"]
        .as_array()
        .expect("states array")
        .iter()
        .position(|state| state["name"] == "review")
        .expect("review state");
    let on_enter = legacy["states"][review_index]["hooks"]["on_enter"]
        .as_array_mut()
        .expect("review on_enter");
    on_enter.retain(|hook| hook["action"] != "carry_review_authority");
    assert_eq!(
        on_enter.len(),
        1,
        "legacy review only dispatches the reviewer"
    );
    let actions = |value: &serde_json::Value| -> Vec<String> {
        value["states"][review_index]["hooks"]["on_enter"]
            .as_array()
            .expect("on_enter")
            .iter()
            .map(|hook| hook["action"].as_str().expect("action").to_owned())
            .collect()
    };
    assert_eq!(
        actions(&current),
        ["carry_review_authority", "dispatch_role_agent"]
    );

    // A human-approval preset has no `on_enter` on review and is left alone.
    let mut approval_only = legacy.clone();
    approval_only["states"][review_index]["hooks"]
        .as_object_mut()
        .expect("hooks object")
        .remove("on_enter");

    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("sqlite pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let now = db::now_rfc3339();
    let mut ids = Vec::new();
    for (name, workflow) in [("legacy", &legacy), ("approval", &approval_only)] {
        let id = db::new_uuid_v4();
        db::ProjectRepo::create(
            &SqliteDb::new(pool.clone()),
            db::CreateProject {
                id: id.clone(),
                name: name.to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: workflow.to_string(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project creates");
        ids.push(id);
    }
    let read = |id: String| {
        let pool = pool.clone();
        async move {
            let raw: String =
                sqlx::query_scalar("SELECT workflow_definition FROM project WHERE id = ?")
                    .bind(id)
                    .fetch_one(&pool)
                    .await
                    .expect("workflow reads");
            serde_json::from_str::<serde_json::Value>(&raw).expect("workflow parses")
        }
    };

    for _ in 0..2 {
        sqlx::raw_sql(update_sql)
            .execute(&pool)
            .await
            .expect("V148 workflow update runs");
        assert_eq!(read(ids[0].clone()).await, current);
        assert_eq!(read(ids[1].clone()).await, approval_only);
    }
}
