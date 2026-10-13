use super::integration_queue_reads::fixture;
use crate::common;
use axum::http::{Method, StatusCode};
use serde_json::{json, Value};
use std::time::Duration;

#[tokio::test]
async fn retry_quarantined_import_through_rest_admits_and_merges_current_branch() {
    let dir = common::TestDir::new("quarantine-rest");
    let h = common::test_app(dir.path(), "quarantine-rest").await;
    let (project, repo) = fixture::project_and_repo(&h, "quarantine").await;
    let task = fixture::task(&h, &project, "missing legacy execution").await;
    let checkout = dir.path().join("target");
    std::fs::create_dir(&checkout).unwrap();
    git::init(&checkout).await.unwrap();
    std::fs::write(checkout.join("base.txt"), "base\n").unwrap();
    git::commit_all(&checkout, "base").await.unwrap();
    git::checkout_branch(&checkout, "main").await.unwrap();
    let tree = dir.path().join("candidate");
    let output = tokio::process::Command::new("git")
        .args([
            "worktree",
            "add",
            "-b",
            "task-branch",
            tree.to_str().unwrap(),
        ])
        .current_dir(&checkout)
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    std::fs::write(tree.join("current.txt"), "current\n").unwrap();
    git::commit_all(&tree, "current branch").await.unwrap();
    let head = git::get_current_sha(&tree).await.unwrap();
    let now = db::now_rfc3339();
    let pool = h.state.db.pool();
    sqlx::query("UPDATE repo SET local_path=? WHERE id=?")
        .bind(checkout.to_str())
        .bind(&repo)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) VALUES('quarantine-target',?,'server',?,'primary_checkout',1,'ready',?,?)")
        .bind(&repo).bind(checkout.to_str()).bind(&now).bind(&now).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES('quarantine-workspace',?,?,?,'task-branch','ready',?,?)")
        .bind(&task).bind(&repo).bind(tree.to_str()).bind(&now).bind(&now).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO workspace_placement(id,workspace_id,task_id,owner_kind,repo_location_id,workspace_handle,generation,state,selected_by,selection_reason,created_at,updated_at) VALUES('quarantine-placement','quarantine-workspace',?,'server','quarantine-target',?,1,'ready','scheduler','{}',?,?)")
        .bind(&task).bind(tree.to_str()).bind(&now).bind(&now).execute(pool).await.unwrap();
    sqlx::query("UPDATE task SET review_passed_at=?,task_state_config=? WHERE id=?")
        .bind(&now)
        .bind(json!({"review":{"ci_steps":["test -f current.txt"]}}).to_string())
        .bind(&task)
        .execute(pool)
        .await
        .unwrap();
    h.state.db.import_integration_pass(100).await.unwrap();
    // This is the start following one that committed the orphan quarantine.
    services::integration_steps::import::import_at_start(&h.state.db)
        .await
        .unwrap();
    h.state.task_service.drain(&task).await.unwrap();
    let detail = fixture::get_json(&h, &format!("/api/v1/tasks/{task}")).await;
    assert_eq!(detail["condition"]["kind"], "parked");
    assert_eq!(detail["integration"]["wait"]["code"], "owner_required");
    assert_eq!(detail["integration"]["action_hint"], "retry");
    assert!(detail["available_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|offer| offer["reason"] == "integration_retry"));
    let _: Value = common::json_request(
        &h.app,
        Method::POST,
        &format!("/api/v1/tasks/{task}/actions"),
        json!({"version":detail["version"],"action":{"verb":"retry"}}),
        StatusCode::OK,
    )
    .await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let detail = fixture::get_json(&h, &format!("/api/v1/tasks/{task}")).await;
        if detail["status"] == "done" {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "{detail}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(git::get_current_sha(&checkout).await.unwrap(), head);
    let candidate: String = sqlx::query_scalar("SELECT original_candidate_sha FROM integration_attempt WHERE task_ref=? AND admission_key LIKE 'admit:%'")
        .bind(&task).fetch_one(pool).await.unwrap();
    assert_eq!(candidate, head);

    let cancelled = fixture::task(&h, &project, "cancel incomplete import").await;
    services::integration_steps::import::import_at_start(&h.state.db)
        .await
        .unwrap();
    h.state.task_service.drain(&cancelled).await.unwrap();
    let detail = fixture::get_json(&h, &format!("/api/v1/tasks/{cancelled}")).await;
    let result: Value = common::json_request(
        &h.app,
        Method::POST,
        &format!("/api/v1/tasks/{cancelled}/actions"),
        json!({"version":detail["version"],"action":{"verb":"cancel"}}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(result["status"], "cancelled");
}
