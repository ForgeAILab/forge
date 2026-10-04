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
    assert_eq!(replayed.outcome, api_types::DeadLetterOutcome::Replayed);
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

#[tokio::test]
async fn unauthenticated_dead_letter_routes_return_401() {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;
    let root = common::TestDir::new("dead-letter-no-auth");
    let harness = test_app(root.path()).await;
    for (method, path) in [
        (Method::GET, "/api/v1/operations/dead-letters"),
        (
            Method::POST,
            "/api/v1/operations/dead-letters/unknown/replay",
        ),
        (
            Method::POST,
            "/api/v1/operations/dead-letters/unknown/dismiss",
        ),
    ] {
        let response = harness
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}

#[tokio::test]
async fn item_replay_returns_specific_conflict_without_counting_an_attempt_and_bodyless_dismiss_works(
) {
    let root = common::TestDir::new("dead-letter-item");
    let harness = test_app(root.path()).await;
    let id = seed(&harness, "notifications", json!({})).await;
    for key in [
        "event:1:commitment:a",
        "event:1:inbox:user",
        "wake-retry:disposition",
    ] {
        sqlx::query("UPDATE worker_dead_letter SET source_key = ? WHERE id = ?")
            .bind(key)
            .bind(&id)
            .execute(harness.state.db.pool())
            .await
            .unwrap();
        let refused = action(&harness, &id, "replay", json!({}), StatusCode::CONFLICT).await;
        assert_eq!(refused["code"], "dead_letter_not_replayable");
        let row = harness.state.db.get_dead_letter(&id).await.unwrap();
        assert_eq!(row.attempts, 8);
        assert_eq!(row.version, 0);
        let listed: DeadLetterListResponse = common::empty_request_with_bearer(
            &harness.app,
            Method::GET,
            "/api/v1/operations/dead-letters",
            &common::admin_jwt(),
            StatusCode::OK,
        )
        .await;
        assert!(!listed.items[0].summary.replayable);
    }
    let actions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM worker_dead_letter_action")
        .fetch_one(harness.state.db.pool())
        .await
        .unwrap();
    assert_eq!(actions, 0);
    let dismissed: DeadLetterActionResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!("/api/v1/operations/dead-letters/{id}/dismiss"),
        &common::admin_jwt(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(dismissed.outcome, api_types::DeadLetterOutcome::Dismissed);
}

#[tokio::test]
async fn resolved_keyset_uses_resolution_time_and_event_context_counts_only_acknowledged_subscribed_rows(
) {
    let root = common::TestDir::new("dead-letter-resolved-order");
    let harness = test_app(root.path()).await;
    let first = seed(&harness, "notifications", json!({})).await;
    let second = seed(&harness, "notifications", json!({})).await;
    let third = seed(&harness, "notifications", json!({})).await;
    let sequence = harness
        .state
        .db
        .get_dead_letter(&third)
        .await
        .unwrap()
        .event_sequence()
        .unwrap();
    sqlx::query(
        "UPDATE event_consumer_cursor SET last_sequence = ? WHERE consumer_name = 'notifications'",
    )
    .bind(sequence)
    .execute(harness.state.db.pool())
    .await
    .unwrap();
    let mut tx = db::begin_immediate(harness.state.db.pool()).await.unwrap();
    harness
        .state
        .db
        .initialize_event_worker_in_tx(
            &mut tx,
            &db::WorkerHealth::new(harness.state.db.clone(), "notifications"),
            &db::EventSubscription::Exact(vec!["notification.requested".into()]),
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let first_row = harness.state.db.get_dead_letter(&first).await.unwrap();
    assert!(first_row.event_created_at.is_some());
    assert_eq!(first_row.events_since, 2);
    // A later ignored event can be scanned/acknowledged, but is not a delivery.
    let ignored = harness
        .state
        .db
        .append_event(CreateDomainEvent {
            id: db::new_uuid_v4(),
            event_type: "ignored".into(),
            entity_type: "test".into(),
            entity_id: "test".into(),
            actor_type: "system".into(),
            actor_id: None,
            scope_type: "system".into(),
            scope_id: "system".into(),
            correlation_id: "test".into(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: None,
            payload_json: "{}".into(),
            created_at: db::now_rfc3339(),
        })
        .await
        .unwrap();
    sqlx::query(
        "UPDATE event_consumer_cursor SET last_sequence = ? WHERE consumer_name = 'notifications'",
    )
    .bind(ignored.sequence)
    .execute(harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(
        harness
            .state
            .db
            .get_dead_letter(&first)
            .await
            .unwrap()
            .events_since,
        2
    );
    let pending = seed(&harness, "notifications", json!({})).await;
    assert_eq!(
        harness
            .state
            .db
            .get_dead_letter(&first)
            .await
            .unwrap()
            .events_since,
        2
    );
    for id in [&first, &second, &third] {
        action(&harness, id, "dismiss", json!({}), StatusCode::OK).await;
    }
    sqlx::query("UPDATE worker_dead_letter SET dead_lettered_at = '2099-01-01T00:00:00Z', resolved_at = '2026-01-01T00:00:00Z' WHERE id = ?").bind(&first).execute(harness.state.db.pool()).await.unwrap();
    for id in [&second, &third] {
        sqlx::query(
            "UPDATE worker_dead_letter SET resolved_at = '2026-10-03T00:00:00Z' WHERE id = ?",
        )
        .bind(id)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    }
    let page: DeadLetterListResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/operations/dead-letters?state=resolved&limit=2",
        &common::admin_jwt(),
        StatusCode::OK,
    )
    .await;
    let mut expected = vec![second, third];
    expected.sort_by(|a, b| b.cmp(a));
    assert_eq!(
        page.items
            .iter()
            .map(|row| row.summary.id.clone())
            .collect::<Vec<_>>(),
        expected
    );
    assert_eq!(
        page.items[0].summary.events_since,
        if page.items[0].summary.id == expected[0]
            && page.items[0].summary.event_sequence == Some(sequence)
        {
            0
        } else {
            1
        }
    );
    let cursor = page.next_cursor.unwrap();
    let last: DeadLetterListResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/operations/dead-letters?state=resolved&limit=2&cursor={cursor}"),
        &common::admin_jwt(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(last.items.len(), 1);
    assert_eq!(last.items[0].summary.id, first);
    assert!(last.next_cursor.is_none());
    assert!(harness
        .state
        .db
        .get_dead_letter(&pending)
        .await
        .unwrap()
        .resolved_at
        .is_none());
}

#[tokio::test]
async fn dropping_request_future_after_commit_does_not_cancel_after_commit() {
    use async_trait::async_trait;
    use services::worker_runtime::{Outcome, Subscription, Worker, WorkerError};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tokio::sync::Notify;
    struct Consumer {
        entered: Notify,
        release: Notify,
        published: Notify,
        calls: AtomicUsize,
    }
    #[async_trait]
    impl Worker for Consumer {
        type Prepared = ();
        fn name(&self) -> &str {
            "cancellation-probe"
        }
        fn subscription(&self) -> Subscription {
            Subscription::All
        }
        async fn handle(&self, _: &db::DomainEvent) -> Result<Outcome<()>, WorkerError> {
            Ok(Outcome::Done(()))
        }
        async fn commit(
            &self,
            _: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
            _: &db::DomainEvent,
            _: &(),
        ) -> Result<(), WorkerError> {
            Ok(())
        }
        async fn after_commit(
            &self,
            _: &db::DomainEvent,
            _: &(),
            _: &(),
        ) -> Result<(), WorkerError> {
            self.entered.notify_one();
            self.release.notified().await;
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.published.notify_one();
            Ok(())
        }
    }
    let root = common::TestDir::new("dead-letter-cancel");
    let mut harness = test_app(root.path()).await;
    let id = seed(&harness, "cancellation-probe", json!({})).await;
    let worker = Arc::new(Consumer {
        entered: Notify::new(),
        release: Notify::new(),
        published: Notify::new(),
        calls: AtomicUsize::new(0),
    });
    let mut service =
        services::dead_letter_service::DeadLetterService::new(harness.state.db.clone());
    service.register(worker.clone());
    harness.state.dead_letter_service = Arc::new(service);
    let state = harness.state.clone();
    let request_id = id.clone();
    let request = tokio::spawn(async move {
        api::routes::operations::replay_dead_letter(
            api::routes::auth::RequireAdmin(api::routes::auth::AuthenticatedUser {
                user_id: "admin".into(),
                email: "admin@example.test".into(),
                is_admin: true,
            }),
            axum::extract::State(state),
            axum::extract::Path(request_id),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), worker.entered.notified())
        .await
        .unwrap();
    assert!(harness
        .state
        .db
        .get_dead_letter(&id)
        .await
        .unwrap()
        .resolved_at
        .is_some());
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    worker.release.notify_one();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        worker.published.notified(),
    )
    .await
    .unwrap();
    assert_eq!(worker.calls.load(Ordering::SeqCst), 1);
}
