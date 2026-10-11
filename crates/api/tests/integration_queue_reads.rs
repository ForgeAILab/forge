//! Public reads of the merge queue: the repository's queue route.
#![allow(dead_code)]
mod common;
#[path = "fixtures/integration_queue.rs"]
mod fixture;
use api_types::{
    IntegrationActionHint, IntegrationMemberState, IntegrationPhase, IntegrationQueueState,
    IntegrationWaitCode,
};
use axum::{body::to_bytes, http::StatusCode};
use fixture::*;

#[tokio::test]
async fn a_repository_without_a_queue_and_a_queue_without_members_read_empty() {
    let dir = common::TestDir::new("iq-empty");
    let h = common::test_app(dir.path(), "iq-empty").await;
    let (_, repo) = project_and_repo(&h, "empty").await;
    let page = queues(&h, &repo, "").await;
    assert!(page.items.is_empty());
    assert!(!page.has_more);
    assert_eq!(page.next_cursor, None);

    let queue = open_queue(&h, &repo, "main").await;
    let page = queues(&h, &repo, "").await;
    assert_eq!(page.items.len(), 1);
    let item = &page.items[0];
    assert_eq!(item.id, queue);
    assert_eq!(item.target_branch, "main");
    assert_eq!(item.state, IntegrationQueueState::Open);
    assert_eq!(item.state_reason, None);
    assert_eq!(item.head_task_id, None);
    assert_eq!(item.member_count, 0);
    assert!(item.members.is_empty());
    assert!(!item.members_truncated);
}

#[tokio::test]
async fn three_members_read_in_queue_order_with_the_head_in_each_phase() {
    let dir = common::TestDir::new("iq-order");
    let h = common::test_app(dir.path(), "iq-order").await;
    let (project, repo) = project_and_repo(&h, "order").await;
    let queue = open_queue(&h, &repo, "main").await;
    let mut tasks = Vec::new();
    let mut attempts = Vec::new();
    for title in ["first", "second", "third"] {
        let id = task(&h, &project, title).await;
        attempts.push(admit(&h, &queue, &project, &id).await);
        tasks.push(id);
    }
    // Nobody is at the head yet: the first waiter has nobody ahead.
    let page = queues(&h, &repo, "").await;
    let members = &page.items[0].members;
    assert_eq!(
        members
            .iter()
            .map(|m| m.task_id.as_str())
            .collect::<Vec<_>>(),
        tasks.iter().map(String::as_str).collect::<Vec<_>>()
    );
    assert_eq!(
        members
            .iter()
            .map(|m| m.integration.position)
            .collect::<Vec<_>>(),
        [Some(0), Some(1), Some(2)]
    );
    assert_eq!(members[0].integration.wait, None);
    assert_eq!(page.items[0].head_task_id, None);

    set_head(&h, &queue, Some(&attempts[0])).await;
    for (state, phase) in [
        ("validating", IntegrationPhase::Validating),
        ("rebasing", IntegrationPhase::Rebasing),
        ("checking", IntegrationPhase::Checking),
        (
            "awaiting_task_step",
            IntegrationPhase::AwaitingAuthorization,
        ),
        ("ready_ff", IntegrationPhase::FastForwarding),
        ("ff_inflight", IntegrationPhase::FastForwarding),
        ("reconciling", IntegrationPhase::Reconciling),
    ] {
        set_attempt(
            &h,
            &attempts[0],
            &format!("state='{state}',started_at='{T0}'"),
        )
        .await;
        let page = queues(&h, &repo, "").await;
        let item = &page.items[0];
        assert_eq!(item.head_task_id.as_deref(), Some(tasks[0].as_str()));
        assert_eq!(item.head_attempt_id.as_deref(), Some(attempts[0].as_str()));
        assert_eq!(item.head_since.as_deref(), Some(T0));
        assert_eq!(item.member_count, 3);
        let head = &item.members[0];
        assert_eq!(head.task_title.as_deref(), Some("first"));
        assert_eq!(head.task_status.as_deref(), Some("merging"));
        assert_eq!(
            head.integration.state,
            IntegrationMemberState::Owned,
            "{state}"
        );
        assert_eq!(head.integration.phase, Some(phase), "{state}");
        assert_eq!(head.integration.position, Some(0));
        assert_eq!(head.integration.wait, None);
        assert_eq!(head.integration.target_branch.as_deref(), Some("main"));
        assert_eq!(head.integration.timings.head_since.as_deref(), Some(T0));
        for (index, waiter) in item.members.iter().enumerate().skip(1) {
            assert_eq!(waiter.task_id, tasks[index]);
            assert_eq!(waiter.integration.state, IntegrationMemberState::Waiting);
            assert_eq!(waiter.integration.phase, None);
            assert_eq!(waiter.integration.position, Some(index as u32));
            assert_eq!(
                waiter.integration.wait.as_ref().map(|wait| wait.code),
                Some(IntegrationWaitCode::Queue)
            );
            assert_eq!(waiter.integration.timings.head_since, None);
        }
    }
}

#[tokio::test]
async fn gated_and_parked_members_carry_their_cause_and_a_suspended_queue_its_reason() {
    let dir = common::TestDir::new("iq-waits");
    let h = common::test_app(dir.path(), "iq-waits").await;
    let (project, repo) = project_and_repo(&h, "waits").await;
    let queue = open_queue(&h, &repo, "main").await;
    let held = task(&h, &project, "held").await;
    let dirty = task(&h, &project, "dirty").await;
    let flaky = task(&h, &project, "flaky").await;
    let waiting = task(&h, &project, "waiting").await;
    let held_attempt = admit(&h, &queue, &project, &held).await;
    let dirty_attempt = admit(&h, &queue, &project, &dirty).await;
    let flaky_attempt = admit(&h, &queue, &project, &flaky).await;
    admit(&h, &queue, &project, &waiting).await;
    set_attempt(&h, &held_attempt, "state='parked',failure_kind='owner_required',failure_message='task_held: the Task is on hold; release it to continue the merge'").await;
    set_attempt(&h, &dirty_attempt, "state='parked',failure_kind='owner_required',failure_message='target_dirty: the target checkout has uncommitted changes'").await;
    set_attempt(&h, &flaky_attempt, "state='parked',failure_kind='infrastructure',failure_message='infrastructure: the owner did not answer',available_at='2026-10-01T00:05:00+00:00'").await;

    let page = queues(&h, &repo, "").await;
    let item = &page.items[0];
    assert_eq!(item.member_count, 4);
    let member = |task: &str| {
        &item
            .members
            .iter()
            .find(|member| member.task_id == task)
            .unwrap()
            .integration
    };
    // A Task stopped by its owner stays "in line": its own stop is the cause.
    let summary = member(&held);
    assert_eq!(summary.state, IntegrationMemberState::Waiting);
    assert_eq!(summary.position, None);
    assert_eq!(
        summary.wait.as_ref().map(|wait| wait.code),
        Some(IntegrationWaitCode::TaskHeld)
    );
    assert_eq!(summary.action_hint, Some(IntegrationActionHint::Release));
    // A parked merge names its cause, its message and the action that exits.
    let summary = member(&dirty);
    assert_eq!(summary.state, IntegrationMemberState::Deferred);
    assert_eq!(summary.position, None);
    let wait = summary.wait.as_ref().unwrap();
    assert_eq!(wait.code, IntegrationWaitCode::TargetDirty);
    assert_eq!(
        wait.message.as_deref(),
        Some("the target checkout has uncommitted changes")
    );
    assert_eq!(wait.retry_at, None);
    assert_eq!(summary.action_hint, Some(IntegrationActionHint::Retry));
    let summary = member(&flaky);
    let wait = summary.wait.as_ref().unwrap();
    assert_eq!(wait.code, IntegrationWaitCode::Infrastructure);
    assert_eq!(wait.retry_at.as_deref(), Some("2026-10-01T00:05:00+00:00"));
    // The one member still in line has nobody ahead of it.
    let summary = member(&waiting);
    assert_eq!(summary.state, IntegrationMemberState::Waiting);
    assert_eq!(summary.position, Some(0));
    assert_eq!(summary.wait, None);

    // A paused Project is the wait of every member in line.
    sqlx::query("UPDATE project SET paused_at=? WHERE id=?")
        .bind(T0)
        .bind(&project)
        .execute(h.state.db.pool())
        .await
        .unwrap();
    let page = queues(&h, &repo, "").await;
    let summary = &page.items[0]
        .members
        .iter()
        .find(|member| member.task_id == waiting)
        .unwrap()
        .integration;
    assert_eq!(
        summary.wait.as_ref().map(|wait| wait.code),
        Some(IntegrationWaitCode::ProjectPaused)
    );
    assert_eq!(
        summary.action_hint,
        Some(IntegrationActionHint::ResumeProject)
    );
    sqlx::query("UPDATE project SET paused_at=NULL WHERE id=?")
        .bind(&project)
        .execute(h.state.db.pool())
        .await
        .unwrap();

    // A suspended queue says why, and its waiters wait for it.
    sqlx::query("UPDATE integration_queue SET state='suspended',last_error_kind='target_unavailable',last_error='the default checkout is not ready' WHERE id=?")
        .bind(&queue)
        .execute(h.state.db.pool())
        .await
        .unwrap();
    let page = queues(&h, &repo, "").await;
    let item = &page.items[0];
    assert_eq!(item.state, IntegrationQueueState::Suspended);
    let reason = item.state_reason.as_ref().unwrap();
    assert_eq!(reason.code, "target_unavailable");
    assert_eq!(
        reason.message.as_deref(),
        Some("the default checkout is not ready")
    );
    let summary = &item
        .members
        .iter()
        .find(|member| member.task_id == waiting)
        .unwrap()
        .integration;
    assert_eq!(summary.queue_state, Some(IntegrationQueueState::Suspended));
    assert_eq!(
        summary.wait.as_ref().map(|wait| wait.code),
        Some(IntegrationWaitCode::QueueSuspended)
    );
}

#[tokio::test]
async fn queues_page_by_target_branch_with_an_opaque_cursor() {
    let dir = common::TestDir::new("iq-page");
    let h = common::test_app(dir.path(), "iq-page").await;
    let (_, repo) = project_and_repo(&h, "page").await;
    for branch in ["release", "main", "hotfix"] {
        open_queue(&h, &repo, branch).await;
    }
    let first = queues(&h, &repo, "?limit=2").await;
    assert_eq!(
        first
            .items
            .iter()
            .map(|queue| queue.target_branch.as_str())
            .collect::<Vec<_>>(),
        ["hotfix", "main"]
    );
    assert!(first.has_more);
    let cursor = first.next_cursor.unwrap();
    let second = queues(&h, &repo, &format!("?limit=2&cursor={cursor}")).await;
    assert_eq!(
        second
            .items
            .iter()
            .map(|queue| queue.target_branch.as_str())
            .collect::<Vec<_>>(),
        ["release"]
    );
    assert!(!second.has_more);
    assert_eq!(second.next_cursor, None);
    let invalid = get_as(
        &h,
        &format!("/api/v1/repos/{repo}/integration-queues?cursor=not-a-cursor"),
        &common::test_jwt(),
        None,
    )
    .await;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_user_without_access_to_the_project_sees_no_queue() {
    let dir = common::TestDir::new("iq-visible");
    let h = common::test_app(dir.path(), "iq-visible").await;
    let (project, repo) = project_and_repo(&h, "private").await;
    let queue = open_queue(&h, &repo, "main").await;
    let id = task(&h, &project, "secret").await;
    admit(&h, &queue, &project, &id).await;
    let uri = format!("/api/v1/repos/{repo}/integration-queues");
    assert_eq!(queues(&h, &repo, "").await.items.len(), 1);

    let now = db::now_rfc3339();
    db::UserRepo::create_user(
        &*h.state.db,
        &db::User {
            id: "outsider".to_owned(),
            email: "outsider@example.com".to_owned(),
            password_hash: "$2b$04$placeholder".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    let response = get_as(&h, &uri, &jwt_for("outsider"), None).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(!body.contains(&id) && !body.contains("secret"), "{body}");
    // The same answer as for a repository that does not exist.
    let unknown = get_as(
        &h,
        "/api/v1/repos/no-such-repo/integration-queues",
        &jwt_for("outsider"),
        None,
    )
    .await;
    assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
}
