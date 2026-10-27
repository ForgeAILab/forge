//! Remote review cancellation, project deletion and reconnect recovery.

use super::*;

#[tokio::test]
async fn connected_cancel_kills_remote_ci_and_supersedes_its_hook_step() {
    use db::TaskStepRepo;
    let fixture = Fixture::new("forge-remote-ci-cancel").await;
    let db = &fixture.harness.state.db;
    let task_id = fixture.resolved.placement.task_id.clone();
    let started = fixture._daemon_root.path().join("cancel-ci-started");
    let completed = fixture._daemon_root.path().join("cancel-ci-completed");
    sqlx::query("UPDATE task SET task_state_config=?,status='merge_failed' WHERE id=?").bind(json!({"review":{"ci_steps":[format!("touch {}; sleep 120; touch {}",quote(&started.to_string_lossy()),quote(&completed.to_string_lossy()))]}}).to_string()).bind(&task_id).execute(db.pool()).await.unwrap();
    let task = TaskRepo::get_by_id(&**db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    let service = fixture.harness.state.task_service.clone();
    let entry = service
        .transition(
            &task_id,
            "review".to_owned(),
            services::task_service::TransitionOptions {
                bridge: Default::default(),
                version: task.version,
                triggered_by: Actor::system(SystemComponent::Workflow),
                reason: Some("remote CI entry".into()),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .unwrap();
    assert!(entry.pending_steps > 0);
    let running = {
        let service = service.clone();
        let id = task_id.clone();
        tokio::spawn(async move { service.drain(&id).await })
    };
    tokio::time::timeout(Duration::from_secs(15), async {
        while !started.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let task = TaskRepo::get_by_id(&**db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    let cancelled = service
        .perform_task_action(
            &task_id,
            TaskAction::Cancel {
                reason: Some("owner cancelled remote CI".into()),
            },
            task.version,
        )
        .await
        .unwrap();
    assert_eq!(cancelled.task.status, "cancelled");
    assert!(!completed.exists());
    assert!(db
        .pending_remote_cancels(None, None)
        .await
        .unwrap()
        .is_empty());
    assert!(db
        .task_steps(&task_id)
        .await
        .unwrap()
        .iter()
        .any(|s| s.kind == "hooks" && s.expected_status == "review" && s.status == "superseded"));
    assert_eq!(
        fixture
            .link
            .as_ref()
            .unwrap()
            .requests(METHOD_WORKSPACE_CANCEL)
            .len(),
        1
    );
    running.await.unwrap().unwrap();
    assert_remote_ci_abandoned(db, &task_id).await;
    assert!(!completed.exists());
    assert_eq!(
        TaskRepo::get_by_id(&**db, &task_id, false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "cancelled"
    );
}

// A manual review re-run of a daemon-placed Task is an ordinary review entry:
// its CI runs on the owner daemon, and cancelling the Task while it runs
// stops the remote command and leaves no cancellation pending, exactly as
// for an automatic entry.
#[tokio::test]
async fn a_review_rerun_on_a_daemon_owned_task_runs_remote_ci_under_the_cancel_fence() {
    use db::TaskStepRepo;
    let fixture = Fixture::new("forge-remote-ci-rerun").await;
    let db = &fixture.harness.state.db;
    let task_id = fixture.resolved.placement.task_id.clone();
    let started = fixture._daemon_root.path().join("rerun-ci-started");
    let completed = fixture._daemon_root.path().join("rerun-ci-completed");
    sqlx::query("UPDATE task SET task_state_config=?,status='review' WHERE id=?").bind(json!({"review":{"ci_steps":[format!("touch {}; sleep 120; touch {}",quote(&started.to_string_lossy()),quote(&completed.to_string_lossy()))]}}).to_string()).bind(&task_id).execute(db.pool()).await.unwrap();
    let service = fixture.harness.state.task_service.clone();
    // The owner's review re-run: the Task re-enters `review` and its CI is
    // asked of the check runner, which runs it on the daemon that owns the
    // worktree.
    let entered = service
        .rerun_review(
            task_id.parse().expect("the Task id is a UUID"),
            api_types::Actor::user(api_types::UserActionSource::Api),
        )
        .await
        .unwrap();
    assert_eq!(entered.status, "review");
    assert!(db.entry_hooks_pending(&task_id).await.unwrap());
    let running = {
        let service = service.clone();
        let id = task_id.clone();
        tokio::spawn(async move { service.drain(&id).await })
    };
    tokio::time::timeout(Duration::from_secs(15), async {
        while !started.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let task = TaskRepo::get_by_id(&**db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    let cancelled = service
        .perform_task_action(
            &task_id,
            TaskAction::Cancel {
                reason: Some("owner cancelled the re-run's remote CI".into()),
            },
            task.version,
        )
        .await
        .unwrap();
    assert_eq!(cancelled.task.status, "cancelled");
    assert!(!completed.exists());
    assert!(db
        .pending_remote_cancels(None, None)
        .await
        .unwrap()
        .is_empty());
    assert!(db
        .task_steps(&task_id)
        .await
        .unwrap()
        .iter()
        .any(|s| s.kind == "hooks" && s.expected_status == "review" && s.status == "superseded"));
    assert_eq!(
        fixture
            .link
            .as_ref()
            .unwrap()
            .requests(METHOD_WORKSPACE_CANCEL)
            .len(),
        1
    );
    running.await.unwrap().unwrap();
    assert_remote_ci_abandoned(db, &task_id).await;
    assert!(!completed.exists());
    assert_eq!(
        TaskRepo::get_by_id(&**db, &task_id, false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "cancelled"
    );
}

#[tokio::test]
async fn disconnected_cancel_fences_workspace_until_real_owner_reconnect_cleanup() {
    let mut fixture = Fixture::new("forge-disconnected-ci-cancel").await;
    let db = fixture.harness.state.db.clone();
    let task_id = fixture.resolved.placement.task_id.clone();
    // Normal admission records this preparation base before publishing ready.
    sqlx::query("UPDATE workspace SET before_sha=? WHERE id=?")
        .bind(&fixture.base_sha)
        .bind(&fixture.resolved.placement.workspace_id)
        .execute(db.pool())
        .await
        .unwrap();

    let started = fixture._daemon_root.path().join("disconnected-ci-started");
    let completed = fixture
        ._daemon_root
        .path()
        .join("disconnected-ci-completed");
    sqlx::query("UPDATE task SET task_state_config=?,status='merge_failed' WHERE id=?").bind(json!({"review":{"ci_steps":[format!("touch {}; sleep 120; touch {}",quote(&started.to_string_lossy()),quote(&completed.to_string_lossy()))]}}).to_string()).bind(&task_id).execute(db.pool()).await.unwrap();
    let service = fixture.harness.state.task_service.clone();
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    service
        .transition(
            &task_id,
            "review".to_owned(),
            services::task_service::TransitionOptions {
                bridge: Default::default(),
                version: task.version,
                triggered_by: Actor::system(SystemComponent::Workflow),
                reason: Some("remote CI".into()),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        while !started.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    fixture.link.take();
    fixture
        .harness
        .state
        .daemon_connections
        .unregister(&fixture.daemon_id);
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    let cancelled = service
        .perform_task_action(&task_id, TaskAction::Cancel { reason: None }, task.version)
        .await
        .unwrap();
    assert_eq!(cancelled.task.status, "cancelled");
    assert!(!completed.exists());
    assert!(db.task_has_pending_remote_cancel(&task_id).await.unwrap());
    assert!(!db
        .workspace_remote_cancels(&fixture.resolved.placement.workspace_id, &task_id)
        .await
        .unwrap()
        .is_empty());
    service.drain(&task_id).await.unwrap();
    // A cancelled Task offers nothing; its workspace stays fenced for reuse.
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(service
        .task_action_offers(&task_id, &Actor::user(UserActionSource::Api))
        .await
        .unwrap()
        .available_actions
        .is_empty());
    assert_eq!(task.status, "cancelled");
    fixture.link = Some(
        DaemonLink::connect(
            &fixture.server,
            &fixture.daemon_id,
            &fixture.token,
            fixture.runtime.clone(),
        )
        .await,
    );
    tokio::time::timeout(Duration::from_secs(15), async {
        while db.task_has_pending_remote_cancel(&task_id).await.unwrap() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    service.drain(&task_id).await.unwrap();
    assert!(!completed.exists());
    assert!(!fixture
        .link
        .as_ref()
        .unwrap()
        .requests(METHOD_WORKSPACE_CANCEL)
        .is_empty());
    assert!(db
        .workspace_remote_cancels(&fixture.resolved.placement.workspace_id, &task_id)
        .await
        .unwrap()
        .is_empty());
    // The fence held while the owner was away; once it confirms, nothing of
    // the abandoned run is left: no waiting consumer, no open review
    // attempt, no run holding a slot, and the Task did not move.
    assert_remote_ci_abandoned(&db, &task_id).await;
    assert!(!completed.exists());
    assert_eq!(
        TaskRepo::get_by_id(&*db, &task_id, false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "cancelled"
    );
}

/// Project deletion while review-entry CI runs on a reachable daemon: the
/// plain delete is refused for the live check run; the forced one stops the
/// run on the daemon, waits for it to settle and only then deletes.
#[tokio::test]
async fn project_delete_stops_a_remote_review_check_on_a_connected_daemon_first() {
    use db::ProjectRepo;
    let fixture = Fixture::new("forge-remote-ci-project-delete").await;
    let db = fixture.harness.state.db.clone();
    let service = fixture.harness.state.task_service.clone();
    let task_id = fixture.resolved.placement.task_id.clone();
    let project_id = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap()
        .project_id;
    let completed = start_remote_review_ci(&fixture, "delete-ci").await;
    match ProjectRepo::ensure_deletable(&*db, &project_id).await {
        Err(db::DbError::ProjectInUse {
            live_check_runs, ..
        }) => assert_eq!(live_check_runs, 1),
        other => panic!("a live check run must refuse deletion, got {other:?}"),
    }
    assert_eq!(live_check_runs(&db, &project_id).await.len(), 1);

    // What `DELETE ...?force=true` does: stop, then the guarded delete, with
    // its bounded retries.
    let mut deleted = false;
    for _ in 0..3 {
        assert!(service
            .project_deletion_unreachable_machines(&project_id)
            .await
            .unwrap()
            .is_empty());
        service
            .prepare_project_deletion(
                &project_id,
                tokio::time::Instant::now() + Duration::from_secs(4),
            )
            .await
            .unwrap();
        match ProjectRepo::delete_with_workspace_paths(&*db, &project_id).await {
            Ok(_) => {
                deleted = true;
                break;
            }
            Err(db::DbError::ProjectInUse { .. }) => {}
            Err(error) => panic!("forced deletion failed: {error}"),
        }
    }
    assert!(deleted, "the remote run was not settled for deletion");
    assert!(
        !fixture
            .link
            .as_ref()
            .unwrap()
            .requests(api_types::METHOD_CHECK_CANCEL)
            .is_empty(),
        "the daemon was told to stop the check before its row was deleted"
    );
    assert!(!completed.exists());
    assert!(ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .is_none());
}

/// The same with the daemon unreachable: the stop cannot be confirmed, so
/// the forced delete is refused too, before it cancels anything. The refusal
/// names the machine; the Task keeps its review entry and its check, and the
/// run's row, which is what fences the remote process, stays. Removing the
/// machine is the way out when it is gone for good: its run is settled and
/// the delete goes through.
#[tokio::test]
async fn project_delete_is_refused_while_an_unreachable_daemon_may_still_run_the_check() {
    use db::ProjectRepo;
    let mut fixture = Fixture::new("forge-remote-ci-project-delete-offline").await;
    let db = fixture.harness.state.db.clone();
    let service = fixture.harness.state.task_service.clone();
    let task_id = fixture.resolved.placement.task_id.clone();
    let project_id = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap()
        .project_id;
    start_remote_review_ci(&fixture, "delete-offline-ci").await;
    fixture.link.take();
    fixture
        .harness
        .state
        .daemon_connections
        .unregister(&fixture.daemon_id);
    let waiting = || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM check_consumer WHERE project_id=? AND cancelled_at IS NULL",
        )
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap()
    };
    let hostname: String = sqlx::query_scalar("SELECT hostname FROM daemon WHERE id=?")
        .bind(&fixture.daemon_id)
        .fetch_one(db.pool())
        .await
        .unwrap();

    // The request itself: refused within the bound, naming the machine, and
    // nothing was cancelled. The fixture's Project was seeded without
    // members; the caller must administer it.
    sqlx::query("INSERT OR IGNORE INTO project_member(id,project_id,user_id,role,created_at,updated_at) SELECT lower(hex(randomblob(16))),?,id,'owner',?,? FROM user")
        .bind(&project_id)
        .bind(db::now_rfc3339())
        .bind(db::now_rfc3339())
        .execute(db.pool())
        .await
        .unwrap();
    let asked = std::time::Instant::now();
    let forced = common::raw_empty_request(
        &fixture.harness.app,
        Method::DELETE,
        &format!("/api/v1/projects/{project_id}?force=true"),
    )
    .await;
    let body: Value = common::parse_response(forced, StatusCode::CONFLICT).await;
    assert!(
        asked.elapsed() <= Duration::from_secs(5),
        "{:?}",
        asked.elapsed()
    );
    assert_eq!(body["code"], "project_in_use");
    assert_eq!(body["details"]["force_cancellation_incomplete"], true);
    assert_eq!(body["details"]["cancelled"], false);
    assert_eq!(body["details"]["unreachable_machines"], json!([hostname]));
    assert_eq!(body["details"]["live_check_runs"], 1);
    assert!(
        body["message"].as_str().unwrap().contains(&hostname),
        "{body}"
    );
    assert!(ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .is_some());
    let live = live_check_runs(&db, &project_id).await;
    assert_eq!(
        live.len(),
        1,
        "the run still fences its operation: {live:?}"
    );
    assert_eq!(
        waiting().await,
        1,
        "a refused delete cancelled the consumer"
    );
    let reviews: Vec<String> = sqlx::query_scalar("SELECT status FROM review WHERE task_id=?")
        .bind(&task_id)
        .fetch_all(db.pool())
        .await
        .unwrap();
    assert!(
        reviews.iter().all(|status| status == "running"),
        "the Task lost its review entry to a refused delete: {reviews:?}"
    );

    // The machine is gone for good: the owner removes it. The check worker
    // settles the run of a removed machine and the delete goes through.
    sqlx::query("UPDATE daemon SET status='offline' WHERE id=?")
        .bind(&fixture.daemon_id)
        .execute(db.pool())
        .await
        .unwrap();
    db.remove_daemon(
        &fixture.daemon_id,
        "owner",
        true,
        &config::embedded_machine_id(),
        false,
    )
    .await
    .expect("the machine is removed");
    assert!(service
        .project_deletion_unreachable_machines(&project_id)
        .await
        .unwrap()
        .is_empty());
    let mut status = StatusCode::CONFLICT;
    for _ in 0..10 {
        status = common::raw_empty_request(
            &fixture.harness.app,
            Method::DELETE,
            &format!("/api/v1/projects/{project_id}?force=true"),
        )
        .await
        .status();
        if status == StatusCode::NO_CONTENT {
            break;
        }
    }
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "removing the machine did not unblock the delete"
    );
    assert!(ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .is_none());
}

/// Hold with the owner disconnected parks the follow-up action on the
/// unconfirmed remote cleanup, naming the machine, until the owner reconnects.
#[tokio::test]
async fn disconnected_hold_parks_follow_up_on_the_named_machine_until_reconnect() {
    let mut fixture = Fixture::new("forge-disconnected-ci-hold").await;
    let db = fixture.harness.state.db.clone();
    let task_id = fixture.resolved.placement.task_id.clone();
    sqlx::query("UPDATE workspace SET before_sha=? WHERE id=?")
        .bind(&fixture.base_sha)
        .bind(&fixture.resolved.placement.workspace_id)
        .execute(db.pool())
        .await
        .unwrap();
    let started = fixture
        ._daemon_root
        .path()
        .join("disconnected-hold-started");
    let completed = fixture
        ._daemon_root
        .path()
        .join("disconnected-hold-completed");
    sqlx::query("UPDATE task SET task_state_config=?,status='merge_failed' WHERE id=?").bind(json!({"review":{"ci_steps":[format!("touch {}; sleep 120; touch {}",quote(&started.to_string_lossy()),quote(&completed.to_string_lossy()))]}}).to_string()).bind(&task_id).execute(db.pool()).await.unwrap();
    let service = fixture.harness.state.task_service.clone();
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    service
        .transition(
            &task_id,
            "review".to_owned(),
            services::task_service::TransitionOptions {
                bridge: Default::default(),
                version: task.version,
                triggered_by: Actor::system(SystemComponent::Workflow),
                reason: Some("remote CI".into()),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        while !started.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    fixture.link.take();
    fixture
        .harness
        .state
        .daemon_connections
        .unregister(&fixture.daemon_id);
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    let held = service
        .perform_task_action(&task_id, TaskAction::Hold { reason: None }, task.version)
        .await
        .unwrap();
    assert_ne!(held.task.status, "cancelled");
    assert!(db.task_has_pending_remote_cancel(&task_id).await.unwrap());
    service.drain(&task_id).await.unwrap();
    let now = db::now_rfc3339();
    TaskRoleAssignmentRepo::assign(
        &*db,
        CreateTaskRoleAssignment {
            id: db::new_uuid_v4(),
            task_id: task_id.clone(),
            role_name: "coder".into(),
            assignee_type: Some(AssigneeKind::User),
            assignee_id: Some(common::fake_daemon::FAKE_DAEMON_USER_ID.into()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    let follow_up = |offers: &TaskActionsResponse| {
        offers
            .available_actions
            .iter()
            .find(|offer| matches!(offer.action.verb(), "release" | "restart" | "retry"))
            .map(|offer| offer.action.clone())
            .unwrap_or_else(|| panic!("held Task offers a follow-up: {offers:?}"))
    };
    let offers = service
        .task_action_offers(&task_id, &Actor::user(UserActionSource::Api))
        .await
        .unwrap();
    let parked = service
        .perform_task_action(&task_id, follow_up(&offers), offers.version)
        .await
        .unwrap();
    let annotation: Value =
        serde_json::from_str(parked.task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["blocking_reason"], "pending_remote_cancel");
    let hostname = db::DaemonRepo::get_by_id(&*db, &fixture.daemon_id)
        .await
        .unwrap()
        .unwrap()
        .hostname;
    assert!(
        annotation["message"]
            .as_str()
            .unwrap()
            .contains(&format!("machine {hostname}")),
        "parked annotation names the machine: {annotation}"
    );
    fixture.link = Some(
        DaemonLink::connect(
            &fixture.server,
            &fixture.daemon_id,
            &fixture.token,
            fixture.runtime.clone(),
        )
        .await,
    );
    tokio::time::timeout(Duration::from_secs(15), async {
        while db.task_has_pending_remote_cancel(&task_id).await.unwrap() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    service.drain(&task_id).await.unwrap();
    assert!(!completed.exists());
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(!task
        .error_annotation
        .as_deref()
        .unwrap_or_default()
        .contains("pending_remote_cancel"));
    // With cleanup confirmed, the follow-up proceeds instead of parking.
    let offers = service
        .task_action_offers(&task_id, &Actor::user(UserActionSource::Api))
        .await
        .unwrap();
    let proceeded = service
        .perform_task_action(&task_id, follow_up(&offers), offers.version)
        .await
        .unwrap();
    assert!(!proceeded
        .task
        .error_annotation
        .as_deref()
        .unwrap_or_default()
        .contains("pending_remote_cancel"));
    assert!(!completed.exists());
}

/// The owning daemon drops off while review-entry CI runs there. That is no
/// verdict: the Task stays in `review` on its typed check wait with the
/// asking step suspended, and when the daemon is back the result it kept is
/// read once and the same review attempt settles.
#[tokio::test]
async fn disconnected_remote_ci_waits_on_the_typed_check_and_resumes_on_reconnect() {
    let mut fixture = Fixture::new("forge-disconnected-ci-resume").await;
    let db = fixture.harness.state.db.clone();
    let task_id = fixture.resolved.placement.task_id.clone();
    let started = fixture._daemon_root.path().join("resume-ci-started");
    let completed = fixture._daemon_root.path().join("resume-ci-completed");
    sqlx::query("UPDATE task SET task_state_config=?,status='merge_failed' WHERE id=?").bind(json!({"review":{"ci_steps":[format!("touch {}; sleep 2; touch {}; printf resumed",quote(&started.to_string_lossy()),quote(&completed.to_string_lossy()))]}}).to_string()).bind(&task_id).execute(db.pool()).await.unwrap();
    let service = fixture.harness.state.task_service.clone();
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    service
        .transition(
            &task_id,
            "review".to_owned(),
            services::task_service::TransitionOptions {
                bridge: Default::default(),
                version: task.version,
                triggered_by: Actor::system(SystemComponent::Workflow),
                reason: Some("remote CI".into()),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        while !started.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    fixture.link.take();
    fixture
        .harness
        .state
        .daemon_connections
        .unregister(&fixture.daemon_id);
    // The command finishes on the daemon while the server cannot hear it.
    tokio::time::timeout(Duration::from_secs(15), async {
        while !completed.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let waiting = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(waiting.status, "review");
    assert!(
        waiting.condition.check_witness().is_some(),
        "the wait is a typed check condition: {:?}",
        waiting.condition
    );
    let suspended: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_step WHERE task_id=? AND kind='hooks' AND status='suspended'",
    )
    .bind(&task_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(suspended, 1);
    assert!(!db.task_has_pending_remote_cancel(&task_id).await.unwrap());
    fixture.link = Some(
        DaemonLink::connect(
            &fixture.server,
            &fixture.daemon_id,
            &fixture.token,
            fixture.runtime.clone(),
        )
        .await,
    );
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            let reviews = ReviewRepo::list_by_task(&*db, &task_id).await.unwrap();
            let details: Value =
                serde_json::from_str(&reviews[0].step_results_json).unwrap_or(Value::Null);
            if details["ci_steps"]
                .as_array()
                .is_some_and(|s| !s.is_empty())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the reconnected daemon's result settles the review attempt");
    let reviews = ReviewRepo::list_by_task(&*db, &task_id).await.unwrap();
    assert_eq!(reviews.len(), 1, "the same attempt resumes");
    let details: Value = serde_json::from_str(&reviews[0].step_results_json).unwrap();
    assert_eq!(details["ci_steps"].as_array().unwrap().len(), 1);
    assert_eq!(details["ci_steps"][0]["exit_code"], 0);
    assert_eq!(details["ci_steps"][0]["output_tail"], "resumed");
    // The command ran once: the reconnected link is only asked what happened.
    assert!(fixture
        .link
        .as_ref()
        .unwrap()
        .requests(api_types::METHOD_CHECK_RUN)
        .is_empty());
}
