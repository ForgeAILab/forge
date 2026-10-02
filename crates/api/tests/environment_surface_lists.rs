#![allow(dead_code)]
mod common;

use api_types::{DaemonRegisterResponse, ProjectResponse};
use axum::{
    body::{to_bytes, Body},
    http::{Method, Request, StatusCode},
};
use serde_json::json;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tower::ServiceExt;
use tracing_subscriber::{
    layer::{Context, SubscriberExt},
    Layer,
};

// The same SQLx db.statement event counter as task_list_read_path.rs. Keep all
// measurements in this file's single test so parallel fixtures cannot add SQL.
#[derive(Clone, Default)]
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
async fn measured_get(
    app: &axum::Router,
    queries: &Queries,
    path: &str,
) -> (Vec<String>, serde_json::Value) {
    queries.statements.lock().unwrap().clear();
    queries.active.store(true, Ordering::Relaxed);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(path)
                .header("authorization", format!("Bearer {}", common::admin_jwt()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    queries.active.store(false, Ordering::Relaxed);
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    (
        queries.statements.lock().unwrap().clone(),
        serde_json::from_slice(&bytes).unwrap(),
    )
}

#[tokio::test]
async fn project_and_agent_list_environment_queries_are_bounded_by_page_not_rows() {
    let queries = Queries::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(queries.clone()))
        .unwrap();
    let mut counts = Vec::new();
    for mode in ["empty", "server", "daemon", "legacy_pause"] {
        let mut by_size = Vec::new();
        for count in [1, 20] {
            let root = tempfile::tempdir().unwrap();
            let h = common::test_app(root.path(), "surface-list-counts").await;
            let daemon: DaemonRegisterResponse = common::json_request(&h.app,Method::POST,"/api/v1/daemons/register",
                json!({"machine_id":"count-machine","hostname":"Count machine","os":"linux","arch":"x86_64","agent_version":"test"}),StatusCode::OK).await;
            for n in 0..count {
                let project: ProjectResponse = common::json_request(
                    &h.app,
                    Method::POST,
                    "/api/v1/projects",
                    json!({"name":format!("Project {n}")}),
                    StatusCode::OK,
                )
                .await;
                if mode == "server" || mode == "daemon" {
                    sqlx::query("INSERT INTO project_machine_readiness (project_id,owner_kind,daemon_id,runtime_id,status,checks_digest,failing_checks_json,output_tail) VALUES (?,?,?,?, 'not_ready','fixture',?, 'not installed')")
                        .bind(&project.id).bind(if mode == "daemon" { "daemon" } else { "server" })
                        .bind(if mode == "daemon" {daemon.daemon_id.as_str()} else {""}).bind(if mode == "daemon" {"runtime-count"} else {""})
                        .bind(json!([{"name":"cargo","output_tail":"not installed"}]).to_string()).execute(h.state.db.pool()).await.unwrap();
                }
                if mode == "legacy_pause" {
                    sqlx::query("UPDATE project SET paused_at='now', system_pause_reason='environment_not_ready',environment_pause_json=? WHERE id=?")
                        .bind(json!({"workspace_id":format!("missing-{n}"),"checks":["cargo"],"role":"coder","output":"not installed","paused_at":"now","last_checked_at":"now","next_check_at":"later"}).to_string())
                        .bind(&project.id).execute(h.state.db.pool()).await.unwrap();
                }
            }
            // Explicitly start with a fresh slot memo, then measure a hit on
            // the same page. Fixture setup cannot change the cold/hit distinction.
            let mut state = (*h.state).clone();
            state.project_slots_memo = Arc::default();
            let app = api::build_router(state, root.path());
            let (cold, body) = measured_get(&app, &queries, "/api/v1/projects?limit=100").await;
            assert_eq!(body["items"].as_array().unwrap().len(), count);
            for project in body["items"].as_array().unwrap() {
                assert_eq!(
                    project["environment_readiness"].as_array().unwrap().len(),
                    usize::from(mode == "server" || mode == "daemon")
                );
                if mode == "daemon" {
                    assert_eq!(
                        project["environment_readiness"][0]["machine"]["name"],
                        "Count machine"
                    );
                }
                if mode == "legacy_pause" {
                    assert_eq!(
                        project["environment_pause"]["machine"]["name"],
                        "Server host"
                    );
                }
            }
            let (warm, _) = measured_get(&app, &queries, "/api/v1/projects?limit=100").await;
            println!(
                "PROJECT {mode} n={count}: cold={} warm={}",
                cold.len(),
                warm.len()
            );
            by_size.push((cold.len(), warm.len()));
        }
        counts.push((mode, by_size));
    }
    for kind in ["cli", "native"] {
        for count in [1, 20] {
            let root = tempfile::tempdir().unwrap();
            let h = common::test_app(root.path(), "agent-list-counts").await;
            let project: ProjectResponse = common::json_request(
                &h.app,
                Method::POST,
                "/api/v1/projects",
                json!({"name":"Agent list"}),
                StatusCode::OK,
            )
            .await;
            for n in 0..count {
                let agent: api_types::AgentResponse = common::json_request(&h.app,Method::POST,"/api/v1/agents",json!({"name":format!("Agent {n}"),"executor_type":"shell","capabilities":["query-counter"]}),StatusCode::OK).await;
                if kind == "native" {
                    let previous =
                        db::AgentProfileRepo::get_profile(&*h.state.db, &agent.profile_id)
                            .await
                            .unwrap()
                            .unwrap();
                    let profile_id = db::new_uuid_v4();
                    let now = db::now_rfc3339();
                    let profile = db::CreateAgentProfile {
                        id: profile_id.clone(),
                        identity_id: agent.id.clone(),
                        backend_kind: "native".into(),
                        executor_type: "embedded".into(),
                        provider: None,
                        model: previous.model,
                        reasoning_effort: previous.reasoning_effort,
                        permission_policy: previous.permission_policy,
                        prompt_template: previous.prompt_template,
                        capabilities_json: previous.capabilities_json,
                        tool_policy_json: previous.tool_policy_json,
                        config_json: previous.config_json,
                        credential_ref: None,
                        daemon_id: None,
                        created_at: now.clone(),
                        updated_at: now.clone(),
                    };
                    db::AgentProfileRepo::create_and_select_profile(
                        &*h.state.db,
                        profile,
                        db::SelectAgentProfile {
                            identity_id: agent.id,
                            profile_id: profile_id.clone(),
                            expected_version: agent.version,
                            updated_at: now,
                        },
                    )
                    .await
                    .unwrap();
                    sqlx::query("INSERT INTO agent_connection_health(profile_id,status,updated_at) VALUES (?,'healthy','now')").bind(&profile_id).execute(h.state.db.pool()).await.unwrap();
                }
            }
            for (label, path) in [
                (
                    "agents",
                    "/api/v1/agents?limit=100&capabilities=query-counter".to_owned(),
                ),
                (
                    "project-agents",
                    format!("/api/v1/projects/{}/agents", project.id),
                ),
            ] {
                let (cold, body) = measured_get(&h.app, &queries, &path).await;
                let size = if label == "agents" {
                    body["items"].as_array().unwrap().len()
                } else {
                    body.as_array().unwrap().len()
                };
                let (warm, _) = measured_get(&h.app, &queries, &path).await;
                let fits = warm
                    .iter()
                    .filter(|sql| {
                        sql.contains("SELECT id FROM daemon WHERE machine_id")
                            || sql.contains("SELECT d.id, r.id")
                            || sql.contains("FROM daemon AS d")
                    })
                    .count();
                assert_eq!(
                    fits, 1,
                    "Agent fit facts must be loaded once per page: {warm:?}"
                );
                let health = warm
                    .iter()
                    .filter(|sql| {
                        sql.contains("SELECT profile_id,status FROM agent_connection_health")
                    })
                    .count();
                assert_eq!(
                    health,
                    usize::from(kind == "native"),
                    "native fit health is batched: {warm:?}"
                );
                println!(
                "AGENT {kind} {label} fixtures={count} returned={size}: cold={} warm={} fit={fits}",
                cold.len(),
                warm.len()
            );
            }
        }
    }
    for (mode, by_size) in counts {
        assert!(by_size[0].0 > 0, "counter must capture SQL");
        assert_eq!(
            by_size[0], by_size[1],
            "Project {mode} statements must not grow with page size"
        );
    }
}
