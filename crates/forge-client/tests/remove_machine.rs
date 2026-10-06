use axum::{http::StatusCode, routing::delete, Json, Router};
use serde_json::{json, Value};
use std::process::Command;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn daemon_remove_sends_delete_prints_result_and_exits_nonzero_on_conflict() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = format!("http://{}", listener.local_addr().unwrap());
    let app=Router::new()
        .route("/api/v1/daemons/dead",delete(||async{Json(json!({"id":"dead","hostname":"Old workstation","pending_remote_cancels_cleared":2,"cleanup_records_cleared":1,"provisioning_attempts_cleared":0,"readiness_records_cleared":0,"placements_failed":1,"tasks_queued":1}))}))
        .route("/api/v1/daemons/live",delete(||async{(StatusCode::CONFLICT,Json(json!({"code":"machine_connected","message":"Stop the daemon before removing this connected machine","details":null})))}));
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
    let table = run(&["daemon", "remove", "dead"]);
    assert!(
        table.status.success(),
        "{}",
        String::from_utf8_lossy(&table.stderr)
    );
    assert!(String::from_utf8_lossy(&table.stdout).contains(
        "Removed machine Old workstation (dead); cleared 2 remote cancellations; queued 1 Tasks"
    ));
    let result = run(&["--output", "json", "daemon", "remove", "dead"]);
    assert!(result.status.success());
    let body: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(body["pending_remote_cancels_cleared"], 2);
    let conflict = run(&["daemon", "remove", "live"]);
    assert_eq!(conflict.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&conflict.stderr).contains("Stop the daemon"));
    job.abort();
}
