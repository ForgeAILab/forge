//! Public reads of the merge queue: the repository's queue route.
#![allow(dead_code)]
use crate::common;
#[path = "../fixtures/integration_queue.rs"]
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
    h.stop_step_worker();
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
    let released = task(&h, &project, "released").await;
    let released_attempt = admit(&h, &queue, &project, &released).await;
    set_attempt(&h, &released_attempt, "state='parked',failure_kind='owner_required',failure_message='task_held: the Task is on hold; release it to continue the merge'").await;
    // The owner's Hold is on the Task row, and the park step has told the
    // Task of the dirty target: both are what the action system reads.
    hold(&h, &held).await;
    tell(
        &h,
        &dirty,
        &deferred(
            &dirty,
            &dirty_attempt,
            "the target checkout has uncommitted changes",
        ),
    )
    .await;
    set_attempt(&h, &held_attempt, "state='parked',failure_kind='owner_required',failure_message='task_held: the Task is on hold; release it to continue the merge'").await;
    set_attempt(&h, &dirty_attempt, "state='parked',failure_kind='owner_required',failure_message='target_dirty: the target checkout has uncommitted changes'").await;
    set_attempt(&h, &flaky_attempt, "state='parked',failure_kind='infrastructure',failure_message='infrastructure: the owner did not answer',available_at='2026-10-01T00:05:00+00:00'").await;

    let page = queues(&h, &repo, "").await;
    let item = &page.items[0];
    assert_eq!(item.member_count, 5);
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
    // The park step has not told this Task yet: the action system would
    // refuse a retry, so none is hinted.
    assert_eq!(summary.action_hint, None);
    // The hint is an offer of the Task itself.
    for (task, verb) in [(&dirty, "retry"), (&flaky, "")] {
        let detail = get_json(&h, &format!("/api/v1/tasks/{task}")).await;
        let offered = detail["available_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|offer| offer["reason"] == "integration_retry");
        assert_eq!(offered, verb == "retry", "{detail}");
        assert_eq!(
            detail["integration"]["action_hint"],
            if offered {
                serde_json::json!("retry")
            } else {
                serde_json::Value::Null
            }
        );
    }
    // Parked under a Hold that was released since: nothing holds the Task
    // now, so no stale cause and no action is said.
    let summary = member(&released);
    assert_eq!(summary.state, IntegrationMemberState::Waiting);
    assert_eq!((summary.wait.clone(), summary.action_hint), (None, None));
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

/// Admission keeps every member of a queue in the queue's own Project. The
/// read does not rest on it: rows of another Project in the line (written
/// here directly, as no writer would) are counted, because they are really
/// ahead, and never listed: no id, no title, not even as the head.
#[tokio::test]
async fn members_of_another_project_in_the_line_are_counted_and_never_listed() {
    let dir = common::TestDir::new("iq-shared");
    let h = common::test_app(dir.path(), "iq-shared").await;
    h.stop_step_worker();
    let (mine, repo) = project_and_repo(&h, "mine").await;
    let now = db::now_rfc3339();
    db::UserRepo::create_user(
        &*h.state.db,
        &db::User {
            id: "stranger".to_owned(),
            email: "stranger@example.com".to_owned(),
            password_hash: "$2b$04$placeholder".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO project(id,name,owner_id,primary_repo_id,created_at,updated_at) VALUES('theirs','theirs','stranger',?,?,?)")
        .bind(&repo)
        .bind(T0)
        .bind(T0)
        .execute(h.state.db.pool())
        .await
        .unwrap();
    let queue = open_queue(&h, &repo, "main").await;
    let secret_head = task(&h, &mine, "secret head").await;
    let first = task(&h, &mine, "first of mine").await;
    let secret_waiter = task(&h, &mine, "secret waiter").await;
    let second = task(&h, &mine, "second of mine").await;
    let deleted = task(&h, &mine, "deleted title").await;
    let head_attempt = admit(&h, &queue, &mine, &secret_head).await;
    admit(&h, &queue, &mine, &first).await;
    let secret_attempt = admit(&h, &queue, &mine, &secret_waiter).await;
    admit(&h, &queue, &mine, &second).await;
    admit(&h, &queue, &mine, &deleted).await;
    for secret in [&secret_head, &secret_waiter] {
        sqlx::query("UPDATE task SET project_id='theirs' WHERE id=?")
            .bind(secret)
            .execute(h.state.db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE integration_attempt SET project_ref='theirs' WHERE task_ref=?")
            .bind(secret)
            .execute(h.state.db.pool())
            .await
            .unwrap();
    }
    set_head(&h, &queue, Some(&head_attempt)).await;
    set_attempt(
        &h,
        &head_attempt,
        &format!("state='rebasing',started_at='{T0}'"),
    )
    .await;
    sqlx::query("UPDATE task SET deleted_at=? WHERE id=?")
        .bind(T0)
        .bind(&deleted)
        .execute(h.state.db.pool())
        .await
        .unwrap();

    let uri = format!("/api/v1/repos/{repo}/integration-queues");
    let response = get_as(&h, &uri, &common::test_jwt(), None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    for hidden in [
        secret_head.as_str(),
        secret_waiter.as_str(),
        head_attempt.as_str(),
        secret_attempt.as_str(),
        "secret",
        "deleted title",
        "theirs",
    ] {
        assert!(!body.contains(hidden), "{hidden} leaked: {body}");
    }
    let page: api_types::IntegrationQueuesResponse = serde_json::from_str(&body).unwrap();
    let item = &page.items[0];
    assert_eq!((item.member_count, item.members.len()), (5, 3));
    assert!(!item.members_truncated);
    // The head is another Project's: the queue has one, and it is not named.
    assert_eq!((&item.head_task_id, &item.head_attempt_id), (&None, &None));
    assert_eq!(item.head_since.as_deref(), Some(T0));
    assert_eq!(
        item.members
            .iter()
            .map(|member| (
                member.task_id.as_str(),
                member.task_title.as_deref(),
                member.integration.position
            ))
            .collect::<Vec<_>>(),
        [
            // The head and the hidden waiter are ahead, and say so.
            (first.as_str(), Some("first of mine"), Some(1)),
            (second.as_str(), Some("second of mine"), Some(3)),
            // A deleted Task keeps its place and its id, and loses its title.
            (deleted.as_str(), None, Some(4)),
        ]
    );
    assert_eq!(item.members[2].task_status, None);
    // The Task read of a visible member counts the same members ahead.
    let detail = get_json(&h, &format!("/api/v1/tasks/{second}")).await;
    assert_eq!(detail["integration"]["position"], 3);
    // The other Project's owner may not read this repository's route at
    // all: the repository is not their Project's own row.
    let theirs = get_as(&h, &uri, &jwt_for("stranger"), None).await;
    assert_eq!(theirs.status(), StatusCode::NOT_FOUND);
    // A cursor is a place in ONE repository's branches: taken from this
    // repository and sent to another, it reads that other repository's own
    // queues after that branch and nothing of this one.
    for branch in ["alpha", "omega"] {
        open_queue(&h, &repo, branch).await;
    }
    let (_, elsewhere) = project_and_repo(&h, "elsewhere").await;
    let zeta = open_queue(&h, &elsewhere, "zeta").await;
    open_queue(&h, &elsewhere, "aaa").await;
    let cursor = queues(&h, &repo, "?limit=2").await.next_cursor.unwrap();
    let other = queues(&h, &elsewhere, &format!("?cursor={cursor}")).await;
    assert_eq!(
        other
            .items
            .iter()
            .map(|queue| queue.id.as_str())
            .collect::<Vec<_>>(),
        [zeta.as_str()]
    );
    // A queue created between two pages is neither repeated nor skips one.
    open_queue(&h, &repo, "beta").await;
    let rest = queues(&h, &repo, &format!("?limit=2&cursor={cursor}")).await;
    assert_eq!(
        rest.items
            .iter()
            .map(|queue| queue.target_branch.as_str())
            .collect::<Vec<_>>(),
        ["omega"]
    );
}
