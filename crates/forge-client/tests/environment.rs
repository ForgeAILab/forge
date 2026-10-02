use axum::{
    extract::Json,
    http::StatusCode,
    routing::{get, post},
    Router,
};
use serde_json::{json, Value};
use std::process::Command;

fn project() -> Value {
    json!({"id":"project-1","name":"Environment","created_at":"2026-10-02T00:00:00Z","updated_at":"2026-10-02T00:00:00Z","version":1,"environment_readiness":[{"machine":machine(),"status":"not_ready","failing_checks":[{"name":"cargo","output_tail":"command not found"}],"output_tail":"command not found","scope_covered":"full","checked_at":null,"next_check_at":null}]})
}
fn machine() -> Value {
    json!({"id":"server","name":"Server host","owner_kind":"server","daemon_id":null,"runtime_id":null})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn environment_commands_print_machine_results_and_http_errors_exit_nonzero() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .route(
            "/api/v1/projects/project-1",
            get(|| async { Json(project()) }),
        )
        .route(
            "/api/v1/projects/project-1/environment/recheck",
            post(recheck_route),
        );
    let job = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let dir = tempfile::tempdir().unwrap();
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_forge-ctl"))
            .env("FORGE_DATA_DIR", dir.path())
            .args(["--server", &server])
            .args(args)
            .output()
            .unwrap()
    };
    let status = run(&["project", "env-status", "project-1"]);
    assert_eq!(status.status.code(), Some(0));
    let output = String::from_utf8(status.stdout).unwrap();
    assert!(output.contains("Server host (server)"));
    assert!(output.contains("not_ready"));
    assert!(output.contains("command not found"));
    let checked = run(&["project", "env-recheck", "project-1", "--machine", "server"]);
    assert_eq!(checked.status.code(), Some(0));
    let output = String::from_utf8(checked.stdout).unwrap();
    assert!(output.contains("cargo: failed (exit 127)"));
    let all = run(&["--output", "json", "project", "env-recheck", "project-1"]);
    assert_eq!(all.status.code(), Some(0));
    let body: Value = serde_json::from_slice(&all.stdout).unwrap();
    assert_eq!(body["machines"][0]["machine"]["id"], "server");
    let unknown = run(&[
        "project",
        "env-recheck",
        "project-1",
        "--machine",
        "missing",
    ]);
    assert_eq!(unknown.status.code(), Some(1));
    assert!(String::from_utf8(unknown.stderr)
        .unwrap()
        .contains("Unknown machine"));
    let error = run(&["project", "env-status", "missing"]);
    assert_eq!(error.status.code(), Some(1));
    assert!(!error.stderr.is_empty());
    job.abort();
}

async fn recheck_route(Json(body): Json<Value>) -> (StatusCode, Json<Value>) {
    if body == json!({"machine":"missing"}) {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"code":"not_found","message":"Unknown machine","details":null})),
        );
    }
    assert!(body == json!({"machine":"server"}) || body == json!({"machine":null}));
    (
        StatusCode::OK,
        Json(
            json!({"machines":[{"machine":machine(),"checks":[{"name":"cargo","passed":false,"exit_code":127,"output_tail":"command not found"}],"error":null}],"project":project()}),
        ),
    )
}
