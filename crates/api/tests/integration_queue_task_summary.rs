//! The `integration` summary on Task detail and Task list items, and the
//! statements the list spends on it. Alone in its test binary: the statement
//! log is process-wide.
#![allow(dead_code)]
mod common;
#[path = "fixtures/integration_queue.rs"]
mod fixture;
use api_types::{IntegrationMemberState, IntegrationPhase, IntegrationWaitCode, TaskResponse};
use axum::{body::to_bytes, http::StatusCode};
use fixture::*;
use serde_json::Value;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tracing_subscriber::{
    layer::{Context, SubscriberExt},
    Layer,
};

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
async fn task_detail_and_list_carry_the_summary_and_the_list_reads_it_in_one_statement() {
    let queries = Queries {
        active: Arc::new(AtomicBool::new(false)),
        statements: Arc::new(Mutex::new(Vec::new())),
    };
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(queries.clone()))
        .unwrap();
    let dir = common::TestDir::new("iq-summary");
    let h = common::test_app(dir.path(), "iq-summary").await;
    h.stop_step_worker();
    let (project, repo) = project_and_repo(&h, "summary").await;
    let queue = open_queue(&h, &repo, "main").await;
    let outside = task(&h, &project, "not queued").await;
    let mut tasks = Vec::new();
    let mut attempts = Vec::new();
    for n in 0..3 {
        let id = task(&h, &project, &format!("queued {n}")).await;
        attempts.push(admit(&h, &queue, &project, &id).await);
        tasks.push(id);
    }
    set_head(&h, &queue, Some(&attempts[0])).await;
    set_attempt(
        &h,
        &attempts[0],
        &format!("state='rebasing',started_at='{T0}'"),
    )
    .await;

    // Detail.
    let head: TaskResponse =
        serde_json::from_value(get_json(&h, &format!("/api/v1/tasks/{}", tasks[0])).await).unwrap();
    let summary = head.integration.unwrap();
    assert_eq!(summary.attempt_id, attempts[0]);
    assert_eq!(summary.queue_id.as_deref(), Some(queue.as_str()));
    assert_eq!(summary.repo_id.as_deref(), Some(repo.as_str()));
    assert_eq!(summary.target_branch.as_deref(), Some("main"));
    assert_eq!(summary.state, IntegrationMemberState::Owned);
    assert_eq!(summary.phase, Some(IntegrationPhase::Rebasing));
    assert_eq!(summary.position, Some(0));
    let third: TaskResponse =
        serde_json::from_value(get_json(&h, &format!("/api/v1/tasks/{}", tasks[2])).await).unwrap();
    let summary = third.integration.unwrap();
    assert_eq!(summary.state, IntegrationMemberState::Waiting);
    assert_eq!(summary.position, Some(2));
    assert_eq!(
        summary.wait.map(|wait| wait.code),
        Some(IntegrationWaitCode::Queue)
    );
    let plain: TaskResponse =
        serde_json::from_value(get_json(&h, &format!("/api/v1/tasks/{outside}")).await).unwrap();
    assert_eq!(plain.integration, None);

    // List: the same summaries, `null` for a Task with no attempt.
    let uri = format!("/api/v1/projects/{project}/tasks?limit=100");
    let list = get_json(&h, &uri).await;
    let item = |id: &str| {
        list["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["id"] == id)
            .unwrap()
            .clone()
    };
    assert_eq!(item(&outside)["integration"], Value::Null);
    assert_eq!(item(&tasks[0])["integration"]["state"], "owned");
    assert_eq!(item(&tasks[0])["integration"]["phase"], "rebasing");
    assert_eq!(item(&tasks[1])["integration"]["position"], 1);
    assert_eq!(item(&tasks[2])["integration"]["position"], 2);
    assert_eq!(item(&tasks[2])["integration"]["wait"]["code"], "queue");

    // No N+1: the list runs the same statements for 4 Tasks and for 24, and
    // exactly one of them reads the attempts.
    let count = |queries: &Queries| {
        let statements = queries.statements.lock().unwrap();
        (
            statements.len(),
            statements
                .iter()
                .filter(|statement| statement.contains("FROM integration_attempt a LEFT JOIN"))
                .count(),
        )
    };
    let measure = || async {
        queries.statements.lock().unwrap().clear();
        queries.active.store(true, Ordering::Relaxed);
        let response = get_as(&h, &uri, &common::test_jwt(), None).await;
        queries.active.store(false, Ordering::Relaxed);
        assert_eq!(response.status(), StatusCode::OK);
        let etag = response.headers()["etag"].to_str().unwrap().to_owned();
        to_bytes(response.into_body(), usize::MAX).await.unwrap();
        // The event relay polls its cursor on its own clock; it is not the list.
        queries
            .statements
            .lock()
            .unwrap()
            .retain(|statement| !statement.contains("event_consumer_cursor"));
        let statements = queries.statements.lock().unwrap().clone();
        (count(&queries), etag, statements)
    };
    let (small, _, small_statements) = measure().await;
    for n in 3..23 {
        let id = task(&h, &project, &format!("queued {n}")).await;
        admit(&h, &queue, &project, &id).await;
    }
    let (large, etag, large_statements) = measure().await;
    assert!(small.0 > 0, "the statement log is not being read");
    assert_eq!(
        small, large,
        "the Task list grew statements with its page: {small_statements:#?} / {large_statements:#?}"
    );
    // The validator's read and the page's read: two, whatever the page holds.
    assert_eq!(large.1, 2);

    // The cached list is refused as soon as the queue moves, although no
    // Task and no Project row was written, and accepted again when idle.
    let idle = get_as(&h, &uri, &common::test_jwt(), Some(&etag)).await;
    assert_eq!(idle.status(), StatusCode::NOT_MODIFIED);
    set_attempt(&h, &attempts[0], "state='checking'").await;
    let moved = get_as(&h, &uri, &common::test_jwt(), Some(&etag)).await;
    assert_eq!(moved.status(), StatusCode::OK);
    let etag = moved.headers()["etag"].to_str().unwrap().to_owned();
    let list: Value =
        serde_json::from_slice(&to_bytes(moved.into_body(), usize::MAX).await.unwrap()).unwrap();
    let head = list["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == tasks[0].as_str())
        .unwrap();
    assert_eq!(head["integration"]["phase"], "checking");
    // The head leaves: every waiter moves up, and the old validator is stale.
    set_head(&h, &queue, None).await;
    let advanced = get_as(&h, &uri, &common::test_jwt(), Some(&etag)).await;
    assert_eq!(advanced.status(), StatusCode::OK);
}
