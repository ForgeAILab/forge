#![allow(dead_code)]

mod common;

use api_types::{
    AgentChatKind, AgentChatSwitcherResponse, AgentInquiryListResponse, AgentInquiryResponse,
    AgentInquiryStatus, AuthResponse, ErrorResponse,
};
use axum::{
    http::{Method, StatusCode},
    Router,
};
use db::{AgentInquiryRepo, CreateAgentInquiry};
use serde_json::json;

/// Ensure the caller's Main Chat exists and return its id. `GET
/// /api/v1/agent-chats` calls `ensure_main_chat` as a side effect, same as
/// `agent_chats.rs`'s own switcher test.
async fn main_chat_id(app: &Router, token: &str) -> String {
    let switcher: AgentChatSwitcherResponse = common::empty_request_with_bearer(
        app,
        Method::GET,
        "/api/v1/agent-chats",
        token,
        StatusCode::OK,
    )
    .await;
    switcher
        .items
        .into_iter()
        .find(|item| item.kind == AgentChatKind::Main)
        .expect("main chat switcher item")
        .chat_id
}

/// Register a brand-new user and return a bearer token for them.
async fn register_user(app: &Router, email: &str) -> String {
    let auth: AuthResponse = common::json_request(
        app,
        Method::POST,
        "/api/v1/auth/register",
        json!({ "email": email, "password": "correct-horse-battery-staple" }),
        StatusCode::CREATED,
    )
    .await;
    auth.access_token
}

/// Insert an `agent_inquiry` row directly -- there is no HTTP endpoint to
/// create one (that's `inquiry.run`, dispatched from inside a Main turn), so
/// tests seed rows the way the runner would.
async fn seed_inquiry(
    db: &db::SqliteDb,
    chat_id: &str,
    owner_user_id: &str,
    title: &str,
) -> db::AgentInquiry {
    AgentInquiryRepo::create_agent_inquiry(
        db,
        CreateAgentInquiry {
            id: db::new_uuid_v4(),
            chat_id: chat_id.to_owned(),
            turn_job_id: None,
            identity_id: "test-identity".to_owned(),
            owner_user_id: owner_user_id.to_owned(),
            title: title.to_owned(),
            question: format!("{title}?"),
            workspace_path: None,
        },
    )
    .await
    .expect("seed agent inquiry")
}

#[tokio::test]
async fn list_agent_chat_inquiries_scopes_by_chat_and_paginates() {
    let workspace = common::TestDir::new("agent-inquiries-list");
    let harness = common::test_app(workspace.path(), "agent-inquiries-list").await;
    let token = common::test_jwt();
    let chat_id = main_chat_id(&harness.app, &token).await;

    for index in 0..3 {
        seed_inquiry(
            &harness.state.db,
            &chat_id,
            "test-user-id",
            &format!("inquiry-{index}"),
        )
        .await;
        // Keyset pagination orders by (created_at DESC, id DESC); a tiny
        // sleep keeps `created_at` from colliding across iterations so the
        // ordering assertions below are unambiguous.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    let first_page: AgentInquiryListResponse = common::json_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/agent-chats/{chat_id}/inquiries?limit=2"),
        &token,
        json!(null),
        StatusCode::OK,
    )
    .await;
    assert_eq!(first_page.items.len(), 2);
    assert!(first_page.has_more);
    let cursor = first_page.next_cursor.expect("next cursor present");

    let second_page: AgentInquiryListResponse = common::json_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/agent-chats/{chat_id}/inquiries?limit=2&cursor={cursor}"),
        &token,
        json!(null),
        StatusCode::OK,
    )
    .await;
    assert_eq!(second_page.items.len(), 1);
    assert!(!second_page.has_more);
    assert!(second_page.next_cursor.is_none());

    // A second user's own chat is a completely different scope: listing it
    // never surfaces the first user's inquiries, and the first user cannot
    // list it at all.
    let other_token = register_user(&harness.app, "agent-inquiries-list-other@example.com").await;
    let other_chat_id = main_chat_id(&harness.app, &other_token).await;
    seed_inquiry(
        &harness.state.db,
        &other_chat_id,
        "someone-else",
        "other-inquiry",
    )
    .await;

    let denied: ErrorResponse = common::json_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/agent-chats/{other_chat_id}/inquiries"),
        &token,
        json!(null),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(denied.code, "not_found");
}

#[tokio::test]
async fn get_agent_inquiry_is_not_found_for_a_different_users_chat() {
    let workspace = common::TestDir::new("agent-inquiries-get");
    let harness = common::test_app(workspace.path(), "agent-inquiries-get").await;
    let token = common::test_jwt();
    let chat_id = main_chat_id(&harness.app, &token).await;
    let inquiry = seed_inquiry(&harness.state.db, &chat_id, "test-user-id", "owned-inquiry").await;

    let fetched: AgentInquiryResponse = common::json_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/inquiries/{}", inquiry.id),
        &token,
        json!(null),
        StatusCode::OK,
    )
    .await;
    assert_eq!(fetched.id, inquiry.id);
    assert_eq!(fetched.chat_id, chat_id);
    assert_eq!(fetched.status, AgentInquiryStatus::Running);
    assert_eq!(fetched.version, 1);

    let other_token = register_user(&harness.app, "agent-inquiries-get-other@example.com").await;
    // A second user, authenticated and otherwise valid, gets exactly the
    // same not-found another id entirely would get: existence is never
    // leaked through a different status code.
    let denied: ErrorResponse = common::json_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/inquiries/{}", inquiry.id),
        &other_token,
        json!(null),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(denied.code, "not_found");

    let unknown_id: ErrorResponse = common::json_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/inquiries/does-not-exist",
        &token,
        json!(null),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(unknown_id.code, "not_found");
}

#[tokio::test]
async fn cancel_agent_inquiry_rejects_stale_version_and_terminal_state() {
    let workspace = common::TestDir::new("agent-inquiries-cancel");
    let harness = common::test_app(workspace.path(), "agent-inquiries-cancel").await;
    let token = common::test_jwt();
    let chat_id = main_chat_id(&harness.app, &token).await;
    let inquiry = seed_inquiry(&harness.state.db, &chat_id, "test-user-id", "cancel-me").await;
    assert_eq!(inquiry.version, 1);

    // A second user cannot cancel someone else's inquiry -- not-found, not
    // forbidden, and the row is left untouched (still cancellable below with
    // the original version).
    let other_token = register_user(&harness.app, "agent-inquiries-cancel-other@example.com").await;
    let denied: ErrorResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!("/api/v1/inquiries/{}/cancel", inquiry.id),
        &other_token,
        json!({ "expected_version": inquiry.version }),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(denied.code, "not_found");

    // Stale version -> 409 version_conflict, not a silent no-op.
    let stale: ErrorResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!("/api/v1/inquiries/{}/cancel", inquiry.id),
        &token,
        json!({ "expected_version": inquiry.version + 1 }),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(stale.code, "version_conflict");

    // Correct version cancels the still-running inquiry.
    let cancelled: AgentInquiryResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!("/api/v1/inquiries/{}/cancel", inquiry.id),
        &token,
        json!({ "expected_version": inquiry.version }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(cancelled.status, AgentInquiryStatus::Cancelled);
    assert_eq!(cancelled.version, inquiry.version + 1);

    // Cancelling an already-terminal inquiry is a conflict, never a silent
    // success that pretends it stopped a run that wasn't running.
    let already_terminal: ErrorResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        &format!("/api/v1/inquiries/{}/cancel", inquiry.id),
        &token,
        json!({ "expected_version": cancelled.version }),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(already_terminal.code, "conflict");
}

/// A sub-agent's activity log is watchable while it runs, and readable after.
/// An inquiry that has not written anything yet is the normal first state, so
/// it must read as an empty page rather than a 404 — otherwise the UI shows
/// an error for every inquiry during its first second.
#[tokio::test]
async fn agent_inquiry_logs_read_as_an_empty_page_before_anything_is_written() {
    let workspace = common::TestDir::new("agent-inquiries-logs");
    let harness = common::test_app(workspace.path(), "agent-inquiries-logs").await;
    let token = common::test_jwt();
    let chat_id = main_chat_id(&harness.app, &token).await;
    let inquiry = seed_inquiry(&harness.state.db, &chat_id, "test-user-id", "Pricing scan").await;

    let page: serde_json::Value = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/inquiries/{}/logs", inquiry.id),
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        page["items"].as_array().map(Vec::len),
        Some(0),
        "an inquiry that has logged nothing yet reads as empty, not missing"
    );
    assert_eq!(page["has_more"], serde_json::json!(false));
}

/// The activity log is authorized exactly like the record it belongs to: a
/// second user gets the same not-found they get for the inquiry itself, so
/// the log never becomes a side channel around chat ownership.
#[tokio::test]
async fn agent_inquiry_logs_are_not_found_for_a_different_users_chat() {
    let workspace = common::TestDir::new("agent-inquiries-logs-authz");
    let harness = common::test_app(workspace.path(), "agent-inquiries-logs-authz").await;
    let owner_token = common::test_jwt();
    let chat_id = main_chat_id(&harness.app, &owner_token).await;
    let inquiry = seed_inquiry(
        &harness.state.db,
        &chat_id,
        "test-user-id",
        "Private research",
    )
    .await;

    let intruder_token = register_user(&harness.app, "inquiry-logs-intruder@example.com").await;
    let denied: ErrorResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/inquiries/{}/logs", inquiry.id),
        &intruder_token,
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(denied.code, "not_found");
}
