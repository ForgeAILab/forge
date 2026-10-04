mod common;
use api_types::TransitionTaskResponse;
use axum::http::{Method, StatusCode};
use db::TaskStepRepo;
use serde_json::json;

#[tokio::test]
async fn transition_returns_requested_commit_and_pending_count_before_final_sse_state() {
    let dir = tempfile::tempdir().unwrap();
    let harness = common::test_app(dir.path(), "task-step").await;
    let state = |name: &str, kind: &str, target: Option<&str>, cascade: bool| {
        json!({
            "name":name,"kind":kind,"column":name,"display_name":name,"role":null,
            "hooks":{"before_exit":[],"on_exit":[],"before_enter":[],"on_enter": if cascade { vec![json!({"action":"auto_cascade_on_completion","params":{},"on_failure":"log"})] } else { vec![] },"after_enter":[]},
            "gate_config":null,"config":{},"triggers":target.map(|to|json!({"accept":{"to":to}})).unwrap_or(json!({}))
        })
    };
    let workflow = json!({"roles":[],"configuration":[],"states":[state("todo","initial",Some("settling"),false),state("settling","gate",Some("done"),true),state("done","terminal",None,false)],"cancellation_state":null});
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO project(id,name,workflow_definition,created_at,updated_at) VALUES ('p','project',?,?,?)")
        .bind(workflow.to_string()).bind(&now).bind(&now).execute(harness.state.db.pool()).await.unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES ('t','p','task','todo',?,?)")
        .bind(&now).bind(&now).execute(harness.state.db.pool()).await.unwrap();
    let mut events = harness.state.event_bus.subscribe();
    let response: TransitionTaskResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/tasks/t/transition",
        json!({"status":"settling","version":1}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(response.task.status, "settling");
    assert_eq!(response.pending_steps, 1);
    assert_eq!(harness.state.db.pending_steps("t").await.unwrap(), 1);
    assert_eq!(
        db::TaskRepo::get_by_id(&*harness.state.db, "t", false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "settling"
    );
    let settled = common::drain(&harness.state, &harness.app, "t").await;
    assert_eq!(settled.status, "done");
    assert_eq!(harness.state.db.pending_steps("t").await.unwrap(), 0);
    let mut saw_final = false;
    while let Ok(event) = events.try_recv() {
        if let events::EventContext::TaskStatusChanged { new_status, .. } = event.context {
            saw_final |= new_status == "done";
        }
    }
    assert!(saw_final, "SSE event bus carries the later final state");
}
