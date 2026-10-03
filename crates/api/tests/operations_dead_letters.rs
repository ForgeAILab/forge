#![allow(dead_code)]
mod common;

use api_types::{DeadLetterActionResponse, DeadLetterListResponse};
use axum::http::{Method, StatusCode};
use db::{CreateDomainEvent, DomainEventRepo};
use serde_json::{json, Value};

struct Harness {
    app: axum::Router,
    state: api::AppState,
    _data: tempfile::TempDir,
}

async fn test_app(root: &std::path::Path) -> Harness {
    let data = tempfile::tempdir().unwrap();
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = std::sync::Arc::new(db::SqliteDb::new(pool));
    let now = db::now_rfc3339();
    db::UserRepo::create_user(
        &*db,
        &db::User {
            id: "test-user-id".into(),
            email: "test@example.com".into(),
            password_hash: "placeholder".into(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    let registry = std::sync::Arc::new(cli_adapters::test_support::test_registry());
    services::ensure_default_agents(&db, &registry)
        .await
        .unwrap();
    let mut config = config::ForgeConfig::with_data_dir(data.path().to_path_buf());
    config.workspace.root = root.to_path_buf();
    // Route tests own explicit delivery; no notification worker races their fixtures.
    let runtime = services::ForgeRuntimeBuilder::from_config(
        db,
        std::sync::Arc::new(events::EventBus::new(64)),
        config,
    )
    .with_adapter_registry(registry)
    .with_jwt_secret(api::state::test_jwt_secret())
    .with_bcrypt_cost(api::state::test_bcrypt_cost())
    .with_workflows_dir(api::state::test_workflows_dir())
    .build();
    let state = api::AppState::from_runtime(runtime, true);
    let app = api::build_router(state.clone(), root.to_path_buf());
    Harness {
        app,
        state,
        _data: data,
    }
}

async fn seed(harness: &Harness, consumer: &str, payload: Value) -> String {
    let event = harness
        .state
        .db
        .append_event(CreateDomainEvent {
            id: db::new_uuid_v4(),
            event_type: "notification.requested".into(),
            entity_type: "notification".into(),
            entity_id: "source".into(),
            actor_type: "system".into(),
            actor_id: None,
            scope_type: "system".into(),
            scope_id: "system".into(),
            correlation_id: "test".into(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: None,
            payload_json: payload.to_string(),
            created_at: db::now_rfc3339(),
        })
        .await
        .unwrap();
    let id = db::new_uuid_v4();
    sqlx::query("INSERT INTO worker_dead_letter (id, worker_name, source_key, item_type, attempts, last_error, first_failed_at, last_failed_at, dead_lettered_at) VALUES (?, ?, ?, 'notification.requested', 8, 'old failure', '2026-10-03T00:00:00Z', '2026-10-03T00:00:00Z', '2026-10-03T00:00:00Z')")
        .bind(&id).bind(consumer).bind(event.sequence.to_string()).execute(harness.state.db.pool()).await.unwrap();
    id
}
async fn action(
    harness: &Harness,
    id: &str,
    action: &str,
    body: Value,
    status: StatusCode,
) -> Value {
    common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!("/api/v1/operations/dead-letters/{id}/{action}"),
        &common::admin_jwt(),
        body,
        status,
    )
    .await
}

#[tokio::test]
async fn dead_letter_routes_require_admin_and_return_not_found_or_conflict() {
    let root = common::TestDir::new("dead-letter-auth");
    let harness = test_app(root.path()).await;
    let id = seed(&harness, "notifications", json!({})).await;
    let _: Value = common::empty_request(
        &harness.app,
        Method::GET,
        "/api/v1/operations/dead-letters",
        StatusCode::FORBIDDEN,
    )
    .await;
    for action in ["replay", "dismiss"] {
        let denied: Value = common::json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/operations/dead-letters/{id}/{action}"),
            json!({}),
            StatusCode::FORBIDDEN,
        )
        .await;
        assert_eq!(denied["code"], "admin_required");
        let missing = self::action(
            &harness,
            "unknown",
            action,
            json!({}),
            StatusCode::NOT_FOUND,
        )
        .await;
        assert_eq!(missing["code"], "not_found");
    }
    let dismissed = action(
        &harness,
        &id,
        "dismiss",
        json!({"reason": "obsolete"}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(dismissed["outcome"], "dismissed");
    assert_eq!(dismissed["dead_letter"]["resolution_reason"], "obsolete");
    for kind in ["replay", "dismiss"] {
        assert_eq!(
            action(&harness, &id, kind, json!({}), StatusCode::CONFLICT).await["code"],
            "version_conflict"
        );
    }
}

#[tokio::test]
async fn dead_letter_list_keysets_filter_state_consumer_and_never_expose_payloads() {
    let root = common::TestDir::new("dead-letter-page");
    let harness = test_app(root.path()).await;
    let mut expected = vec![];
    for _ in 0..3 {
        expected.push(seed(&harness, "consumer-a", json!({"secret": "payload-body"})).await);
    }
    let other = seed(&harness, "consumer-b", json!({})).await;
    expected.sort_by(|a, b| b.cmp(a));
    let first: DeadLetterListResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/operations/dead-letters?consumer=consumer-a&limit=2",
        &common::admin_jwt(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(first.items.len(), 2);
    assert_eq!(first.items[0].summary.id, expected[0]);
    let cursor = first.next_cursor.unwrap();
    assert!(!cursor.contains(&expected[1]));
    // Deleting the boundary row still permits the remaining keyset page.
    sqlx::query("DELETE FROM worker_dead_letter WHERE id = ?")
        .bind(&expected[1])
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let second: DeadLetterListResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/operations/dead-letters?consumer=consumer-a&limit=2&cursor={cursor}"),
        &common::admin_jwt(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(second.items.len(), 1);
    assert_eq!(second.items[0].summary.id, expected[2]);
    assert!(second.next_cursor.is_none());
    let _: Value = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/operations/dead-letters?cursor=bad!",
        &common::admin_jwt(),
        StatusCode::BAD_REQUEST,
    )
    .await;
    let _: Value = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/operations/dead-letters?consumer=consumer-b&cursor={cursor}"),
        &common::admin_jwt(),
        StatusCode::BAD_REQUEST,
    )
    .await;
    action(&harness, &other, "dismiss", json!({}), StatusCode::OK).await;
    let resolved: Value = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/operations/dead-letters?state=resolved",
        &common::admin_jwt(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(resolved["items"].as_array().unwrap().len(), 1);
    assert_eq!(resolved["items"][0]["summary"]["id"], other);
    let open: Value = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/operations/dead-letters?state=open",
        &common::admin_jwt(),
        StatusCode::OK,
    )
    .await;
    assert!(!open.to_string().contains("payload-body"));
    assert!(!open.to_string().contains("payload_json"));
    assert_eq!(open["items"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn dead_letter_replay_reports_failure_then_success_and_keeps_cursor() {
    let root = common::TestDir::new("dead-letter-replay");
    let harness = test_app(root.path()).await;
    let consumer = "notifications";
    let id = seed(
        &harness,
        consumer,
        json!({"secret": "not an actual request"}),
    )
    .await;
    sqlx::query("INSERT INTO event_consumer_cursor (consumer_name, last_sequence, updated_at) VALUES (?, 100, '2000-01-01T00:00:00Z') ON CONFLICT(consumer_name) DO UPDATE SET last_sequence = 100, updated_at = excluded.updated_at").bind(consumer).execute(harness.state.db.pool()).await.unwrap();
    let failed = action(&harness, &id, "replay", json!({}), StatusCode::OK).await;
    assert_eq!(failed["outcome"], "replay_failed");
    assert_eq!(failed["dead_letter"]["state"], "open");
    assert_eq!(failed["dead_letter"]["summary"]["attempts"], 9);
    // Fix the source fixture to simulate a corrected preparation cause.
    let project: api_types::ProjectResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({"name": "Dead letter project"}),
        StatusCode::OK,
    )
    .await;
    let row = harness.state.db.get_dead_letter(&id).await.unwrap();
    sqlx::query("UPDATE domain_event SET payload_json = ? WHERE sequence = ?")
        .bind(
            json!({"project_id": project.id, "event_type": "test", "title": "Recovered"})
                .to_string(),
        )
        .bind(row.event_sequence().unwrap())
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let replayed: DeadLetterActionResponse =
        serde_json::from_value(action(&harness, &id, "replay", json!({}), StatusCode::OK).await)
            .unwrap();
    assert_eq!(replayed.outcome, "replayed");
    assert!(replayed.dead_letter.resolved_by.is_some());
    let notifications: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM notification WHERE title = 'Recovered'")
            .fetch_one(harness.state.db.pool())
            .await
            .unwrap();
    assert_eq!(notifications, 1);
    let cursor = harness
        .state
        .db
        .get_consumer_cursor(consumer)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor.last_sequence, 100);
    action(&harness, &id, "replay", json!({}), StatusCode::CONFLICT).await;
}
