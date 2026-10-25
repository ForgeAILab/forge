//! Tests for MCP task mutations and typed task commands.

use super::*;

#[tokio::test]
async fn update_task_publishes_one_scoped_event_after_commit_and_none_on_conflict() {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(db::SqliteDb::new(pool));
    let state = AppState::new(db.clone(), Arc::new(events::EventBus::new(16)));
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES ('p','p',?,?)")
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES ('t','p','task','todo',?,?)")
        .bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    let mut events = state.event_bus.subscribe();
    let params = || {
        serde_json::from_value(json!({
            "task_id": "t", "version": 1, "title": "Updated title",
            "description": "Updated description", "priority": 4, "plan": "Updated plan"
        }))
        .unwrap()
    };
    let response = forge_update_task(&state, params()).await.unwrap();
    assert_eq!(response["title"], "Updated title");
    assert_eq!(response["description"], "Updated description");
    assert_eq!(response["priority"], 4);
    assert_eq!(response["plan"], "Updated plan");
    let event = events
        .try_recv()
        .expect("the committed edit notifies clients");
    assert_eq!(event.event_type, "task.updated");
    assert_eq!(event.entity_id, "t");
    assert!(
        matches!(event.context, events::EventContext::TaskUpdated { project_id } if project_id == "p")
    );
    let stored = TaskRepo::get_by_id(&*db, "t", false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.title, "Updated title");
    assert_eq!(stored.version, 2);
    assert!(events.try_recv().is_err(), "publish exactly once");
    assert!(forge_update_task(&state, params()).await.is_err());
    assert!(
        events.try_recv().is_err(),
        "a rejected CAS publishes nothing"
    );
}

#[test]
fn typed_task_commands_match_domain_serde_acceptance_and_convert_without_loss() {
    use std::collections::{BTreeMap, BTreeSet};
    let contract = &operation_registry::mcp::lookup("forge_task_action")
        .unwrap()
        .input
        .schema["properties"]["action"];
    let domain = api_types::task_action_schema();
    let fields = |schema: &Value| -> BTreeMap<String, BTreeSet<String>> {
        schema["oneOf"]
            .as_array()
            .unwrap()
            .iter()
            .map(|variant| {
                let verb = variant["properties"]["verb"]["const"]
                    .as_str()
                    .or_else(|| variant["properties"]["verb"]["enum"][0].as_str())
                    .unwrap()
                    .to_owned();
                (
                    verb,
                    variant["properties"]
                        .as_object()
                        .unwrap()
                        .keys()
                        .cloned()
                        .collect(),
                )
            })
            .collect()
    };
    assert_eq!(
        fields(contract),
        fields(&domain),
        "every domain condition verb and field has a MCP input row"
    );
    for action in [
        json!({"verb":"start"}),
        json!({"verb":"hold","reason":null}),
        json!({"verb":"release","reason":"resume"}),
        json!({"verb":"retry","reason":"again","fresh_session":true,"refresh_workspace":false,"reset_budget":true,"guidance":"continue"}),
        json!({"verb":"send_back","guidance":"fix"}),
        json!({"verb":"approve","override":true,"reason":null}),
        json!({"verb":"restart","reason":"again"}),
        json!({"verb":"cancel"}),
    ] {
        let expected: api_types::TaskAction = serde_json::from_value(action.clone()).unwrap();
        let decoded = operation_registry::mcp::lookup("forge_task_action")
            .unwrap()
            .decode(json!({"task_id":"t","action":action,"version":1}))
            .unwrap();
        let operation_registry::mcp::McpInput::TaskAction(params) = decoded else {
            panic!("typed action")
        };
        assert_eq!(task_action_input(params.action), expected);
    }
    // The one acceptance difference, listed as a break: Serde ignores
    // extra fields on an internally tagged unit variant, so REST accepts
    // this `start`; the closed MCP contract refuses it.
    let padded_start = json!({"verb":"start","reason":"now"});
    assert!(serde_json::from_value::<api_types::TaskAction>(padded_start.clone()).is_ok());
    assert!(operation_registry::mcp::lookup("forge_task_action")
        .unwrap()
        .decode(json!({"task_id":"t","action":padded_start,"version":1}))
        .is_err());
    for action in [
        json!({"verb":"send_back"}),
        json!({"verb":"cancel","reset_budget":true}),
        json!({"verb":"new-integration-verb"}),
        json!({"verb":"retry","unknown":true}),
    ] {
        assert!(serde_json::from_value::<api_types::TaskAction>(action.clone()).is_err());
        assert!(operation_registry::mcp::lookup("forge_task_action")
            .unwrap()
            .decode(json!({"task_id":"t","action":action,"version":1}))
            .is_err());
    }
}

#[tokio::test]
async fn transition_response_keeps_committed_task_after_background_advance() {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(db::SqliteDb::new(pool));
    let state = AppState::new(db.clone(), Arc::new(events::EventBus::new(16)));
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES ('p','p',?,?)")
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES ('t','p','task','todo',?,?)")
        .bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    let committed = TaskRepo::get_by_id(&*db, "t", false)
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE task SET status='done',version=version+1 WHERE id='t'")
        .execute(db.pool())
        .await
        .unwrap();
    let value = transition_task_value(
        &state,
        committed,
        1,
        &Actor::delegated_user("mcp-test-user", api_types::UserActionSource::Api),
    )
    .await
    .unwrap();
    assert_eq!(value["status"], "todo");
    assert_eq!(value["version"], 1);
    assert_eq!(value["pending_steps"], 1);
    assert!(value["available_actions"].is_array());
    assert_eq!(
        TaskRepo::get_by_id(&*db, "t", false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "done"
    );
}
