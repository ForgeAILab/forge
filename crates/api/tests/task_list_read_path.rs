#![allow(dead_code)]
mod common;
use api_types::{ProjectResponse, TaskResponse};
use axum::{
    body::{to_bytes, Body},
    http::{Method, Request, StatusCode},
    response::Response,
};
use serde_json::json;
use services::task_usage_fixture as usage_fixture;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tower::ServiceExt;
use tracing_subscriber::{
    layer::{Context, SubscriberExt},
    Layer,
};

async fn get(h: &common::Harness, uri: &str, etag: Option<&str>) -> Response {
    let mut request = Request::builder()
        .uri(uri)
        .header("authorization", format!("Bearer {}", common::test_jwt()));
    if let Some(etag) = etag {
        request = request.header("if-none-match", etag);
    }
    h.app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap()
}
async fn project(h: &common::Harness) -> ProjectResponse {
    common::json_request(
        &h.app,
        Method::POST,
        "/api/v1/projects",
        json!({"name":"Read path"}),
        StatusCode::OK,
    )
    .await
}
async fn task(h: &common::Harness, project: &str) -> TaskResponse {
    common::json_request(
        &h.app,
        Method::POST,
        &format!("/api/v1/projects/{project}/tasks"),
        json!({"title":"Read path"}),
        StatusCode::OK,
    )
    .await
}
fn tag(response: &Response) -> String {
    response.headers()["etag"].to_str().unwrap().to_owned()
}

#[tokio::test]
async fn task_list_conditional_get_and_parameter_identity() {
    let dir = common::TestDir::new("task-list-conditional");
    let h = common::test_app(dir.path(), "task-list-conditional").await;
    let p = project(&h).await;
    task(&h, &p.id).await;
    let uri = format!("/api/v1/projects/{}/tasks", p.id);
    let first = get(&h, &uri, None).await;
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.headers()["cache-control"], "private, no-cache");
    assert_eq!(first.headers()["vary"], "Authorization");
    let etag = tag(&first);
    let second = get(&h, &uri, Some(&etag)).await;
    assert_eq!(second.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(tag(&second), etag);
    assert_eq!(second.headers()["cache-control"], "private, no-cache");
    assert_eq!(second.headers()["vary"], "Authorization");
    assert!(to_bytes(second.into_body(), usize::MAX)
        .await
        .unwrap()
        .is_empty());
    // Weak comparison, tag lists and wildcard are HTTP validators too.
    for matching in [
        format!("\"other\", {etag}"),
        etag.trim_start_matches("W/").to_owned(),
        "*".to_owned(),
    ] {
        assert_eq!(
            get(&h, &uri, Some(&matching)).await.status(),
            StatusCode::NOT_MODIFIED
        );
    }
    let mut tags = std::collections::HashSet::from([etag.clone()]);
    for params in [
        "status=todo",
        "canonical_phase=backlog",
        "q=Read",
        "sort_by=title",
        "sort_by=title&sort_order=desc",
        "limit=1",
        "include_total=true",
        "include_archived=true",
        "priority=1",
    ] {
        let response = get(&h, &format!("{uri}?{params}"), Some(&etag)).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(tags.insert(tag(&response)), "distinct {params}");
    }
    let other = project(&h).await;
    task(&h, &other.id).await;
    assert_eq!(
        get(&h, &uri, Some(&etag)).await.status(),
        StatusCode::NOT_MODIFIED
    );
    task(&h, &p.id).await;
    let page = get(&h, &format!("{uri}?limit=1"), None).await;
    let page_tag = tag(&page);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(page.into_body(), usize::MAX).await.unwrap()).unwrap();
    let cursor = body["next_cursor"].as_str().unwrap();
    assert_ne!(
        tag(&get(&h, &format!("{uri}?limit=1&cursor={cursor}"), None).await),
        page_tag
    );
}

#[tokio::test]
async fn task_list_revisions_cover_persisted_projection_sources() {
    let dir = common::TestDir::new("task-list-fields");
    let h = common::test_app(dir.path(), "task-list-fields").await;
    let p = project(&h).await;
    let t = task(&h, &p.id).await;
    let parent = task(&h, &p.id).await;
    let uri = format!(
        "/api/v1/projects/{}/tasks?include_archived=true&include_cancelled=true",
        p.id
    );
    let mut etag = tag(&get(&h, &uri, None).await);
    // All task-backed list fields, plus description/filter and config/metadata inputs.
    for (column, value) in [
        ("title", "Changed"),
        ("task_type", "discovery"),
        ("status", "done"),
        ("priority", "3"),
        ("board_position", "20"),
        ("subtask_order", "2"),
        ("assignee_type", "user"),
        ("assignee_id", "test-user-id"),
        ("parent_task_id", parent.id.as_str()),
        ("error_annotation", "{}"),
        (
            "blocked_json",
            "{\"reason\":\"blocked\",\"created_at\":\"now\"}",
        ),
        (
            "failed_json",
            "{\"reason\":\"failed\",\"created_at\":\"now\"}",
        ),
        ("metadata_json", "{\"awaiting_human\":true}"),
        ("review_passed_at", "2026-10-01T00:00:00Z"),
        ("archived_at", "2026-10-01T00:00:00Z"),
        ("version", "20"),
        ("created_at", "2026-09-01T00:00:00Z"),
        ("updated_at", "2026-10-01T00:00:00Z"),
        ("description", "Filter input"),
        ("task_state_config", "{}"),
    ] {
        sqlx::query(&if column == "assignee_type" {
            "UPDATE task SET assignee_type = ?, assignee_id = 'original' WHERE id = ?".to_owned()
        } else {
            format!("UPDATE task SET {column} = ? WHERE id = ?")
        })
        .bind(value)
        .bind(&t.id)
        .execute(h.state.db.pool())
        .await
        .unwrap();
        h.state
            .db
            .check_task_conditions_of(std::slice::from_ref(&t.id))
            .await
            .unwrap();
        let response = get(&h, &uri, Some(&etag)).await;
        assert_eq!(response.status(), StatusCode::OK, "{column}");
        let next = tag(&response);
        assert_ne!(next, etag, "{column}");
        etag = next;
    }
    let exec = db::new_uuid_v4();
    let review = db::new_uuid_v4();
    for sql in [
        format!("INSERT INTO execution (id, task_id, role, status, created_at, updated_at) VALUES ('{exec}', '{}', 'coder', 'running', '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z')", t.id),
        format!("UPDATE execution SET status = 'completed' WHERE id = '{exec}'"),
        format!("INSERT INTO review (id, task_id, execution_id, attempt_number, status, created_at, updated_at, started_at) VALUES ('{review}', '{}', '{exec}', 1, 'running', 'now', 'now', 'now')", t.id),
        format!("UPDATE review SET status = 'awaiting_human' WHERE id = '{review}'"),
        format!("DELETE FROM review WHERE id = '{review}'"),
        format!("DELETE FROM execution WHERE id = '{exec}'"),
        format!("INSERT INTO task_role_assignment (id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at) VALUES ('role', '{}', 'coder', 'user', 'test-user-id', 'now', 'now')", t.id),
        "UPDATE task_role_assignment SET assignee_id = 'changed' WHERE id = 'role'".to_owned(),
        format!("INSERT INTO transition_log (id, task_id, from_state, to_state, triggered_by, trigger_reason, rejection, created_at) VALUES ('transition', '{}', 'review', 'in_progress', 'system:test', 'retry', 1, 'now')", t.id),
        format!("UPDATE project SET workflow_definition = '{{}}' WHERE id = '{}'", p.id),
        "DELETE FROM task_role_assignment WHERE id = 'role'".to_owned(),
    ] {
        sqlx::query(&sql).execute(h.state.db.pool()).await.unwrap();
        h.state.db.check_task_conditions_of(std::slice::from_ref(&t.id)).await.unwrap();
        let response = get(&h, &uri, Some(&etag)).await;
        assert_eq!(response.status(), StatusCode::OK, "{sql}");
        let next = tag(&response); assert_ne!(next, etag, "{sql}"); etag = next;
    }
    sqlx::query("INSERT INTO project_integration (id, project_id, platform, base_url, owner, repo, token_secret_ref, created_at, updated_at) VALUES ('integration', ?, 'github', 'https://example.test', 'owner', 'repo', 'secret', 'now', 'now')").bind(&p.id).execute(h.state.db.pool()).await.unwrap();
    for sql in [
        format!("INSERT INTO task_external_link (id, task_id, integration_id, platform, remote_owner, remote_repo, remote_issue_number, remote_url, global_id, synced_at, created_at, updated_at) VALUES ('link', '{}', 'integration', 'github', 'owner', 'repo', 1, 'https://example.test/1', 'global', 'now', 'now', 'now')", t.id),
        "UPDATE task_external_link SET remote_issue_number = 2 WHERE id = 'link'".to_owned(),
        "UPDATE task_external_link SET remote_url = 'https://example.test/2' WHERE id = 'link'".to_owned(),
        "DELETE FROM task_external_link WHERE id = 'link'".to_owned(),
    ] {
        sqlx::query(&sql).execute(h.state.db.pool()).await.unwrap();
        h.state.db.check_task_conditions_of(std::slice::from_ref(&t.id)).await.unwrap();
        let response = get(&h, &uri, Some(&etag)).await;
        assert_eq!(response.status(), StatusCode::OK);
        let next = tag(&response); assert_ne!(next, etag); etag = next;
    }
    // Clock-dependent retry health must always bypass conditional responses.
    sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
        .bind(json!({"deferred_dispatch":{"not_before":"2099-01-01T00:00:00Z", "reason":"retry", "target_state":"in_progress"}}).to_string()).bind(&t.id).execute(h.state.db.pool()).await.unwrap();
    h.state
        .db
        .check_task_conditions_of(std::slice::from_ref(&t.id))
        .await
        .unwrap();
    let response = get(&h, &uri, Some("*")).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get("etag").is_none());
    sqlx::query("UPDATE task SET metadata_json = NULL WHERE id = ?")
        .bind(&t.id)
        .execute(h.state.db.pool())
        .await
        .unwrap();
    h.state
        .db
        .check_task_conditions_of(std::slice::from_ref(&t.id))
        .await
        .unwrap();
    assert_eq!(
        get(&h, &uri, Some("*")).await.status(),
        StatusCode::NOT_MODIFIED
    );
}

#[tokio::test]
async fn task_list_heartbeat_progress_and_logs_preserve_revisions_and_validator() {
    let dir = common::TestDir::new("task-list-heartbeat");
    let h = common::test_app(dir.path(), "task-list-heartbeat").await;
    let p = project(&h).await;
    let t = task(&h, &p.id).await;
    let exec = db::new_uuid_v4();
    sqlx::query("INSERT INTO execution (id, task_id, role, status, lease_owner, lease_expires_at, created_at, updated_at) VALUES (?, ?, 'coder', 'running', 'reader', '2099-01-01T00:00:00Z', '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z')")
        .bind(&exec).bind(&t.id).execute(h.state.db.pool()).await.unwrap();
    let uri = format!("/api/v1/projects/{}/tasks", p.id);
    let first = get(&h, &uri, None).await;
    let etag = tag(&first);
    let revisions: (i64, i64) =
        sqlx::query_as("SELECT board_revision, list_revision FROM project WHERE id = ?")
            .bind(&p.id)
            .fetch_one(h.state.db.pool())
            .await
            .unwrap();
    let renewed = db::ExecutionRepo::renew_lease(
        &*h.state.db,
        db::RenewExecutionLease {
            execution_id: exec.clone(),
            expected_version: 1,
            owner: "reader".to_owned(),
            lease_expires_at: "2099-01-02T00:00:00Z".to_owned(),
            now: "2026-10-01T00:00:20Z".to_owned(),
        },
    )
    .await
    .unwrap();
    let db::ExecutionLeaseMutation::Updated(renewed) = renewed else {
        panic!("lease renewed")
    };
    let progress = db::ExecutionRepo::record_progress(
        &*h.state.db,
        db::RecordExecutionProgress {
            execution_id: exec.clone(),
            expected_version: renewed.execution_version,
            owner: "reader".to_owned(),
            progress_at: "2026-10-01T00:00:21Z".to_owned(),
            now: "2026-10-01T00:00:21Z".to_owned(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(progress, db::ExecutionLeaseMutation::Updated(_)));
    sqlx::query("UPDATE execution SET logs_path = 'execution.jsonl', updated_at = '2026-10-01T00:00:22Z' WHERE id = ?")
        .bind(&exec).execute(h.state.db.pool()).await.unwrap();
    let after: (i64, i64) =
        sqlx::query_as("SELECT board_revision, list_revision FROM project WHERE id = ?")
            .bind(&p.id)
            .fetch_one(h.state.db.pool())
            .await
            .unwrap();
    assert_eq!(after, revisions);
    let conditional = get(&h, &uri, Some(&etag)).await;
    assert_eq!(conditional.status(), StatusCode::NOT_MODIFIED);
    assert!(to_bytes(conditional.into_body(), usize::MAX)
        .await
        .unwrap()
        .is_empty());
}

#[derive(Clone)]
struct Queries {
    active: Arc<AtomicBool>,
    statements: Arc<Mutex<Vec<String>>>,
}
impl<S: tracing::Subscriber> Layer<S> for Queries {
    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        if !self.active.load(Ordering::Relaxed) || event.metadata().target() != "sqlx::query" {
            return;
        }
        struct Statement(String);
        impl tracing::field::Visit for Statement {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "db.statement" {
                    self.0 = format!("{value:?}");
                }
            }
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                if field.name() == "db.statement" && !value.is_empty() {
                    self.0 = value.to_owned();
                }
                if field.name() == "summary" && self.0.is_empty() {
                    self.0 = value.to_owned();
                }
            }
        }
        let mut statement = Statement(String::new());
        event.record(&mut statement);
        self.statements.lock().unwrap().push(statement.0);
    }
}

#[tokio::test]
#[ignore = "small benchmark; run explicitly with --ignored --nocapture"]
async fn task_list_read_path_bench() {
    let queries = Queries {
        active: Arc::new(AtomicBool::new(false)),
        statements: Arc::new(Mutex::new(Vec::new())),
    };
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(queries.clone()))
        .unwrap();
    let dir = common::TestDir::new("task-read-bench");
    let h = common::test_app(dir.path(), "task-read-bench").await;
    let p = project(&h).await;
    let mut task_ids = Vec::new();
    for n in 0..200 {
        let task = db::new_uuid_v4();
        sqlx::query("INSERT INTO task (id, project_id, title, status, created_at, updated_at) VALUES (?, ?, ?, 'todo', '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z')").bind(&task).bind(&p.id).bind(format!("Task {n}")).execute(h.state.db.pool()).await.unwrap();
        for attempt in 0..3 {
            let exec = db::new_uuid_v4();
            sqlx::query("INSERT INTO execution (id, task_id, role, status, created_at, updated_at) VALUES (?, ?, 'coder', 'completed', '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z')").bind(&exec).bind(&task).execute(h.state.db.pool()).await.unwrap();
            usage_fixture::seed(&h.state.db, &task, &exec, attempt, 5).await;
        }
        task_ids.push(task);
    }
    let uri = format!("/api/v1/projects/{}/tasks?limit=100", p.id);
    let etag = tag(&get(&h, &uri, None).await);
    let endpoints = [
        ("list", uri.clone(), false),
        (
            "detail",
            format!("/api/v1/tasks/{}/detail", task_ids[0]),
            false,
        ),
        ("conditional-list", uri, true),
    ];
    let mut timings = [Vec::new(), Vec::new(), Vec::new()];
    let mut counts = [Vec::new(), Vec::new(), Vec::new()];
    // Warm every endpoint on the same immutable fixture, then interleave 40
    // measured requests per endpoint so ordering does not bias the comparison.
    for round in 0..45 {
        for (index, (label, path, conditional)) in endpoints.iter().enumerate() {
            queries.statements.lock().unwrap().clear();
            queries.active.store(round >= 5, Ordering::Relaxed);
            let start = std::time::Instant::now();
            let response = get(&h, path, if *conditional { Some(&etag) } else { None }).await;
            let status = response.status();
            to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let elapsed = start.elapsed();
            queries.active.store(false, Ordering::Relaxed);
            assert_eq!(
                status,
                if *conditional {
                    StatusCode::NOT_MODIFIED
                } else {
                    StatusCode::OK
                },
                "{label}"
            );
            if round < 5 {
                continue;
            }
            let statements = queries.statements.lock().unwrap();
            timings[index].push(elapsed);
            counts[index].push(statements.len());
            if *conditional {
                assert!(
                    !statements.iter().any(|s| (s.contains(" FROM task ")
                        || s.contains("FROM task WHERE"))
                        && !s.contains("SELECT EXISTS")),
                    "304 must not read task rows: {statements:?}"
                );
            }
        }
    }
    for (index, (label, _, _)) in endpoints.iter().enumerate() {
        timings[index].sort();
        counts[index].sort();
        let median = (timings[index][19] + timings[index][20]) / 2;
        println!(
            "{label}: median={median:?}; warmed=5; runs=40; queries={}..{}",
            counts[index][0], counts[index][39]
        );
    }
}

#[tokio::test]
async fn condition_serialization_and_truthful_human_wait_agree_in_list_and_detail() {
    let dir = common::TestDir::new("condition-wire");
    let h = common::test_app(dir.path(), "condition-wire").await;
    let p = project(&h).await;
    let t = task(&h, &p.id).await;
    h.stop_step_worker();
    sqlx::query("UPDATE task SET metadata_json='{\"awaiting_human\":true,\"awaiting_human_reason\":\"plan_review\"}' WHERE id=?").bind(&t.id).execute(h.state.db.pool()).await.unwrap();
    h.state
        .db
        .check_task_conditions_of(std::slice::from_ref(&t.id))
        .await
        .unwrap();
    let list = get(&h, &format!("/api/v1/projects/{}/tasks", p.id), None).await;
    assert_eq!(list.status(), StatusCode::OK);
    let list: serde_json::Value =
        serde_json::from_slice(&to_bytes(list.into_body(), usize::MAX).await.unwrap()).unwrap();
    let detail = get(&h, &format!("/api/v1/tasks/{}", t.id), None).await;
    assert_eq!(detail.status(), StatusCode::OK);
    let detail: serde_json::Value =
        serde_json::from_slice(&to_bytes(detail.into_body(), usize::MAX).await.unwrap()).unwrap();
    let item = &list["items"][0];
    for value in [item, &detail] {
        for removed in ["error_annotation", "blocked", "failed"] {
            assert!(value.get(removed).is_none(), "{removed}");
        }
        assert_eq!(
            value["awaiting_human"], true,
            "deliberate difference: truthful_list_awaiting_human"
        );
        assert_eq!(value["condition"]["details"]["human_wait"], true);
        assert_eq!(value["condition"]["kind"], "parked");
        assert_eq!(value["condition"]["primary"]["kind"], "human_decision");
        assert!(value["condition"].get("evidence").is_none());
    }
    assert_eq!(item["condition"], detail["condition"]);
    assert_eq!(item["workflow_health"], detail["workflow_health"]);
    assert_eq!(item["workflow_exception"], detail["workflow_exception"]);
}
