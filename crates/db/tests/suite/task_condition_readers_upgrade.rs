//! Replay the reader migration on a database written before it: the visible
//! half of an owner park (the dispatcher's bridge annotation) moves into the
//! durable park with its message and `blocked_at`, whether or not the park
//! row existed, and reads back exactly as it was shown.
use db::{create_sqlite_pool, new_uuid_v4, run_migrations_from, TaskRepo};
use serde_json::{json, Value};
use std::path::PathBuf;

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("forge-task-condition-readers-upgrade")
        .join(format!("{name}-{}", new_uuid_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

const BLOCKED_AT: &str = "2026-10-06T01:00:00Z";
const CAUSE: &str = "the Project workflow does not define this state";
const WORKFLOW_INVALID: &str = "Nothing can continue this Task from `retired_state`: the Project workflow does not define this state. Owner: the Project Agent. Action: edit the Project workflow or move the Task to a state it defines.";
const UNKNOWN_CONDITION: &str = "Nothing owns this Task: no recorded owner for entry hooks. Owner: the Project owner. Action: move the Task back to the previous state and forward again, or cancel it.";

fn bridge(reason: &str, message: &str) -> String {
    json!({"type":"workflow_guard_rejected","blocking_reason":reason,"blocked_by":"system:task_dispatcher","blocked_at":BLOCKED_AT,"blocked_execution_id":null,"artifact":null,"message":message}).to_string()
}

#[tokio::test]
async fn bridge_annotations_become_parks_that_read_as_they_were_shown() {
    let full = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"));
    let base = scratch("base-migrations");
    for entry in std::fs::read_dir(&full).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        if name.as_str() < "V202610070507" {
            std::fs::copy(entry.path(), base.join(&name)).unwrap();
        }
    }
    let pool = create_sqlite_pool(&format!(
        "sqlite://{}",
        scratch("db").join("forge.sqlite").display()
    ))
    .await
    .unwrap();
    run_migrations_from(&pool, &base).await.unwrap();
    let now = "2026-10-06T00:00:00Z";
    sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES ('p','p',?,?)")
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();
    // The park exactly as the dispatcher encodes it.
    let invalid_park = json!({"reason":{"WorkflowInvalid":{"state":"retired_state","cause":CAUSE}},"owner":"ProjectAgent","recovery":"EditWorkflow"});
    let unknown_park = json!({"reason":{"UnknownCondition":{"owner":"entry hooks"}},"owner":"Workflow","recovery":"ReconcileEntry"});
    // (id, status, annotation, park row, deleted)
    let rows: Vec<(&str, &str, String, Option<&Value>, bool)> = vec![
        ("invalid_parked", "retired_state", bridge("workflow_invalid", WORKFLOW_INVALID), Some(&invalid_park), false),
        ("invalid_crashed", "retired_state", bridge("workflow_invalid", WORKFLOW_INVALID), None, false),
        ("unknown_parked", "review", bridge("unknown_condition", UNKNOWN_CONDITION), Some(&unknown_park), false),
        ("unknown_crashed", "review", bridge("unknown_condition", UNKNOWN_CONDITION), None, false),
        ("deleted", "retired_state", bridge("workflow_invalid", WORKFLOW_INVALID), None, true),
        // Somebody else's rejection is not a bridge and stays where it is.
        ("user_guard", "in_progress", json!({"type":"workflow_guard_rejected","blocking_reason":"guard said no","blocked_by":"user:alice","message":"a real guard rejection"}).to_string(), None, false),
    ];
    for (id, status, annotation, park, deleted) in &rows {
        sqlx::query("INSERT INTO task(id,project_id,title,status,priority,created_at,updated_at,error_annotation,deleted_at) VALUES (?,'p','t',?,0,?,?,?,?)")
            .bind(id).bind(status).bind(now).bind(now).bind(annotation).bind(deleted.then_some(now))
            .execute(&pool).await.unwrap();
        if let Some(park) = park {
            sqlx::query("INSERT INTO task_schedule_park(task_id,epoch,reason_json) SELECT id,status_epoch,? FROM task WHERE id=?")
                .bind(park.to_string()).bind(id).execute(&pool).await.unwrap();
        }
    }

    run_migrations_from(&pool, &full).await.unwrap();

    let db = db::SqliteDb::new(pool.clone());
    let park = |id: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, String>(
                "SELECT reason_json FROM task_schedule_park WHERE task_id=?",
            )
            .bind(id)
            .fetch_optional(&pool)
            .await
            .unwrap()
            .map(|raw| serde_json::from_str::<Value>(&raw).unwrap())
        }
    };
    for (id, message, expected_park, reason) in [
        (
            "invalid_parked",
            WORKFLOW_INVALID,
            &invalid_park,
            "workflow_invalid",
        ),
        (
            "invalid_crashed",
            WORKFLOW_INVALID,
            &invalid_park,
            "workflow_invalid",
        ),
        (
            "unknown_parked",
            UNKNOWN_CONDITION,
            &unknown_park,
            "unknown_condition",
        ),
        (
            "unknown_crashed",
            UNKNOWN_CONDITION,
            &unknown_park,
            "unknown_condition",
        ),
    ] {
        let task = TaskRepo::get_by_id(&db, id, false).await.unwrap().unwrap();
        assert_eq!(task.error_annotation, None, "{id}: the bridge is removed");
        let public = serde_json::to_value(task.condition.public()).unwrap();
        let diagnostic = &public["details"]["diagnostic"];
        // Shown once, as it was stored: not rebuilt around its own text.
        assert_eq!(diagnostic["message"], message, "{id}");
        assert_eq!(diagnostic["blocked_at"], BLOCKED_AT, "{id}");
        assert_eq!(diagnostic["blocking_reason"], reason, "{id}");
        assert_eq!(public["kind"], "parked", "{id}");
        if reason == "workflow_invalid" {
            assert_eq!(public["primary"]["cause"], CAUSE, "{id}: {public}");
        }
        // But for the saved diagnostic, the row is the dispatcher's own park:
        // its next pass finds nothing to rewrite.
        let mut stored = park(id).await.unwrap();
        assert_eq!(
            stored
                .as_object_mut()
                .unwrap()
                .remove("diagnostic")
                .unwrap()["blocked_at"],
            BLOCKED_AT,
            "{id}"
        );
        assert_eq!(&stored, expected_park, "{id}");
    }
    assert_eq!(park("deleted").await, None, "a deleted Task gets no park");
    assert_eq!(park("user_guard").await, None);
    let user = TaskRepo::get_by_id(&db, "user_guard", false)
        .await
        .unwrap()
        .unwrap();
    assert!(user.error_annotation.is_some());

    // A second run is a no-op.
    let before: Vec<(String, String)> =
        sqlx::query_as("SELECT task_id,reason_json FROM task_schedule_park ORDER BY task_id")
            .fetch_all(&pool)
            .await
            .unwrap();
    run_migrations_from(&pool, &full).await.unwrap();
    let after: Vec<(String, String)> =
        sqlx::query_as("SELECT task_id,reason_json FROM task_schedule_park ORDER BY task_id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(before, after);
}
