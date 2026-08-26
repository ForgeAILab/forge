//! 8.4.2 — event-publication audit.
//!
//! Every committed durable command must publish its event after the
//! transaction that committed it, never before or inside it. This file
//! proves that for the commands this change owns, and documents where the
//! same guarantee now comes from a generic mechanism rather than a bespoke
//! `event_bus.publish` call.
//!
//! Two publication styles exist in this codebase:
//!
//! - **Direct**: the route calls `state.event_bus.publish(ForgeEvent { .. })`
//!   itself, immediately after its mutation returns `Ok`. Project creation
//!   (`project.created`) and Project deletion (`project.deleted`) use this
//!   style and were already correct — `project_creation_publishes_directly`
//!   and `project_deletion_publishes_directly` are regression coverage, not
//!   a fix.
//! - **Outbox**: the command appends a row to the durable `domain_event`
//!   table (`DomainEventRepo::append_event_in_tx`) from inside a larger
//!   composite transaction — Project creation *from a Charter approval*,
//!   Main Genesis control transfer, every Agent Chat message/turn, and
//!   milestone readiness/release all use this style. Before this change
//!   nothing ever drained that table to `EventBus`: the row was durable and
//!   correct, but no SSE frame was ever sent for it, regardless of how the
//!   browser routed frames it did receive. `services::DomainEventBroadcastConsumer`
//!   (`crates/services/src/domain_event_broadcast.rs`) is the fix — a
//!   generic post-commit relay, not a bespoke publish per command, because
//!   the outbox is written from many places including Task/review and
//!   baseline/reconciliation commands this change does not own.
//!
//! `agent_chat_message_send_is_durable_and_broadcasts_after_drain` proves
//! the outbox style end-to-end over real HTTP for the "messages/turns"
//! domain: nothing reaches `EventBus` from the send itself, and the
//! consumer relays it correctly afterward. `domain_event_broadcast_audit`
//! proves the same relay + routing-relevant `scope_type`/`entity_type`
//! shape for Project creation from a Charter approval, Main Genesis control
//! transfer, and milestone readiness/release without re-deriving each
//! command's full fixture chain (`crates/services/tests/main_project_create_command.rs`
//! and `crates/services/src/domain_event_broadcast.rs`'s own unit test
//! already exercise those end-to-end). Task/review and baseline/reconciliation
//! commands write through the identical `append_event_in_tx` path from
//! `crates/db/src/sqlite/{task,execution,review,task_adaptive}.rs`, owned by
//! another agent's in-flight files in this change; they are covered by the
//! same generic consumer and are audited here structurally (the "task"
//! scope case in `domain_event_broadcast_audit`) rather than end-to-end.
//!
//! `web/src/api/sse.ts`'s `routeDomainEventCommitted` is what turns
//! `scope_type`/`entity_type`/`scope_id` into the exact query keys each
//! scope affects; `web/src/api/sse.test.ts` covers that half of the
//! contract this file's Rust side establishes.

mod common;

use api_types::{ConnectedEmbeddedAgentResponse, MainAgentBindingResponse, ProjectResponse};
use axum::http::{Method, StatusCode};
use db::{new_uuid_v4, now_rfc3339, CreateDomainEvent, DomainEventRepo};
use events::{EventContext, ForgeEvent};
use serde_json::json;
use services::DomainEventBroadcastConsumer;
use std::time::Duration;

async fn connect_main_agent(
    app: &axum::Router,
    token: &str,
    name: &str,
) -> ConnectedEmbeddedAgentResponse {
    common::connect_embedded_agent(
        app,
        token,
        name,
        "event-audit",
        "fixture-secret",
        json!({"permissions": ["read_account", "read_project", "handoff"]}),
        json!({"allowed": ["read_account", "read_project", "handoff"]}),
    )
    .await
}

/// Absence check: within a short bounded window, no frame arrives. Used to
/// establish the "before the fix" baseline a command's committed outbox row
/// alone would have left the browser in.
async fn assert_no_event(rx: &mut tokio::sync::broadcast::Receiver<ForgeEvent>) {
    let outcome = tokio::time::timeout(Duration::from_millis(150), rx.recv()).await;
    assert!(
        outcome.is_err(),
        "expected no event within the bounded window, got {outcome:?}"
    );
}

async fn recv_event(rx: &mut tokio::sync::broadcast::Receiver<ForgeEvent>) -> ForgeEvent {
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("event arrives within the timeout")
        .expect("event channel stays open")
}

#[tokio::test]
async fn project_creation_publishes_directly() {
    let workspace = common::TestDir::new("event-audit-project-creation");
    let harness = common::test_app(workspace.path(), "event-audit-project-creation").await;
    let mut rx = harness.state.event_bus.subscribe();

    let project: ProjectResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Event Audit Project" }),
        StatusCode::OK,
    )
    .await;

    let received = recv_event(&mut rx).await;
    assert_eq!(received.event_type, "project.created");
    assert_eq!(received.entity_id, project.id);
}

#[tokio::test]
async fn project_deletion_publishes_directly() {
    let workspace = common::TestDir::new("event-audit-project-deletion");
    let harness = common::test_app(workspace.path(), "event-audit-project-deletion").await;

    let project: ProjectResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Event Audit Project" }),
        StatusCode::OK,
    )
    .await;

    let mut rx = harness.state.event_bus.subscribe();
    let response = common::raw_empty_request(
        &harness.app,
        Method::DELETE,
        &format!("/api/v1/projects/{}", project.id),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let received = recv_event(&mut rx).await;
    assert_eq!(received.event_type, "project.deleted");
    assert_eq!(received.entity_id, project.id);
}

/// "messages/turns": sending a Main Chat message writes
/// `agent_chat.message.admitted` to the `domain_event` outbox
/// (`crates/db/src/sqlite/agent_chat.rs::append_agent_chat_event`) from
/// inside the same transaction as the message/turn insert — a committed
/// command publishing no event was exactly the gap `DomainEventBroadcastConsumer`
/// closes.
#[tokio::test]
async fn agent_chat_message_send_is_durable_and_broadcasts_after_drain() {
    let workspace = common::TestDir::new("event-audit-agent-chat-message");
    let harness = common::test_app(workspace.path(), "event-audit-agent-chat-message").await;
    let token = common::test_jwt();

    let connected = connect_main_agent(&harness.app, &token, "event-audit-main").await;
    let binding: MainAgentBindingResponse = common::json_request_with_bearer(
        &harness.app,
        Method::PUT,
        "/api/v1/account/main-agent",
        &token,
        json!({
            "identity_id": connected.agent.id,
            "profile_id": connected.profile.id,
            "expected_version": 0,
            "autonomy_policy": {}
        }),
        StatusCode::OK,
    )
    .await;

    let consumer = DomainEventBroadcastConsumer::new(
        std::sync::Arc::clone(&harness.state.db),
        std::sync::Arc::clone(&harness.state.event_bus),
    );
    // Connecting and binding the agent may itself have written unrelated
    // domain events (e.g. the binding's own admission). Advance the
    // consumer's cursor past them before subscribing, so the assertions
    // below are about the message send alone.
    consumer
        .broadcast_once(100)
        .await
        .expect("priming drain succeeds");

    let mut rx = harness.state.event_bus.subscribe();

    let _sent: api_types::SendAgentChatMessageResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!("/api/v1/agent-chats/{}/messages", binding.chat_id),
        &token,
        json!({ "content": "Event publication audit message", "dedupe_key": null }),
        StatusCode::CREATED,
    )
    .await;

    // Reproduces the gap: the send committed (the message exists, the
    // route returned 201), but nothing reached `EventBus` on its own.
    assert_no_event(&mut rx).await;

    let published = consumer
        .broadcast_once(10)
        .await
        .expect("broadcast succeeds");
    assert!(published >= 1);

    // The exact scope is what `web/src/api/sse.ts`'s
    // `routeDomainEventCommitted` keys on to invalidate the Agent Chat query
    // family for `binding.chat_id` — a wrong scope here would route to
    // nothing, the audit's "routes to the wrong keys" failure mode.
    for _ in 0..published {
        let received = recv_event(&mut rx).await;
        assert_eq!(received.event_type, "domain_event.committed");
        let EventContext::DomainEventCommitted {
            scope_type,
            scope_id,
            entity_type,
            ..
        } = received.context
        else {
            panic!("expected DomainEventCommitted");
        };
        assert_eq!(scope_type, "agent_chat");
        assert_eq!(scope_id, binding.chat_id);
        assert_eq!(entity_type, "agent_chat_message");
    }
}

/// Structural audit for the outbox-style commands whose full fixture chain
/// is exercised elsewhere (see the module doc comment): each case reproduces
/// exactly the `domain_event` row shape its owning command writes today
/// (verified by reading the source, not guessed) and proves
/// `DomainEventBroadcastConsumer` relays it with the `scope_type`/
/// `entity_type` the client routes on.
#[tokio::test]
async fn domain_event_broadcast_audit() {
    let workspace = common::TestDir::new("event-audit-outbox-scopes");
    let harness = common::test_app(workspace.path(), "event-audit-outbox-scopes").await;
    let consumer = DomainEventBroadcastConsumer::new(
        std::sync::Arc::clone(&harness.state.db),
        std::sync::Arc::clone(&harness.state.event_bus),
    );

    struct Case {
        event_type: &'static str,
        entity_type: &'static str,
        entity_id: &'static str,
        scope_type: &'static str,
        scope_id: &'static str,
        source: &'static str,
    }

    let cases = [
        Case {
            // crates/db/src/sqlite/orchestration.rs — Project creation from
            // a Charter approval.
            event_type: "project.created_from_charter_approval",
            entity_type: "project",
            entity_id: "audit-project-1",
            scope_type: "project",
            scope_id: "audit-project-1",
            source: "crates/db/src/sqlite/orchestration.rs",
        },
        Case {
            // crates/db/src/sqlite/agent_chat.rs — Main Genesis control
            // transfer.
            event_type: "agent_chat.turn.control_transferred",
            entity_type: "agent_chat_turn_job",
            entity_id: "audit-turn-1",
            scope_type: "agent_chat",
            scope_id: "audit-chat-1",
            source: "crates/db/src/sqlite/agent_chat.rs",
        },
        Case {
            // crates/services/src/milestone_runtime.rs — readiness/release.
            event_type: "milestone.released",
            entity_type: "milestone",
            entity_id: "audit-milestone-1",
            scope_type: "project",
            scope_id: "audit-project-2",
            source: "crates/services/src/milestone_runtime.rs",
        },
        Case {
            // crates/db/src/sqlite/task_adaptive.rs — the adaptive-boundary
            // conflict this change's baseline/reconciliation work builds
            // on; owned by another agent's in-flight files here.
            event_type: "task.adaptive_boundary_crossed",
            entity_type: "task",
            entity_id: "audit-task-1",
            scope_type: "project",
            scope_id: "audit-project-3",
            source: "crates/db/src/sqlite/task_adaptive.rs",
        },
        Case {
            // crates/db/src/sqlite/task.rs / review.rs — Task/review,
            // scoped to the Task rather than the Project.
            event_type: "task.transitioned",
            entity_type: "task",
            entity_id: "audit-task-2",
            scope_type: "task",
            scope_id: "audit-task-2",
            source: "crates/db/src/sqlite/task.rs",
        },
    ];

    for case in cases {
        let mut rx = harness.state.event_bus.subscribe();
        DomainEventRepo::append_event(
            &*harness.state.db,
            CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: case.event_type.to_owned(),
                entity_type: case.entity_type.to_owned(),
                entity_id: case.entity_id.to_owned(),
                actor_type: "system".to_owned(),
                actor_id: None,
                scope_type: case.scope_type.to_owned(),
                scope_id: case.scope_id.to_owned(),
                correlation_id: new_uuid_v4(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: Some(format!("event-audit:{}", case.entity_id)),
                payload_json: "{}".to_owned(),
                created_at: now_rfc3339(),
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{} event appends: {error}", case.source));

        // Before drain: the committed row alone reaches no one — the exact
        // gap this audit exists to close.
        assert_no_event(&mut rx).await;

        consumer
            .broadcast_once(10)
            .await
            .unwrap_or_else(|error| panic!("{} broadcasts: {error}", case.source));

        let received = recv_event(&mut rx).await;
        assert_eq!(received.event_type, "domain_event.committed");
        match received.context {
            EventContext::DomainEventCommitted {
                scope_type,
                scope_id,
                entity_type,
                ..
            } => {
                assert_eq!(scope_type, case.scope_type, "{}", case.source);
                assert_eq!(scope_id, case.scope_id, "{}", case.source);
                assert_eq!(entity_type, case.entity_type, "{}", case.source);
            }
            other => panic!(
                "{}: expected DomainEventCommitted, got {other:?}",
                case.source
            ),
        }
    }
}
