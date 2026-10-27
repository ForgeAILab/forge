//! Tests for execution recovery in the task dispatcher.

use super::*;
/// A run that died without its failure being recorded (full disk) leaves the
/// Task in its working state with a failed newest execution and no blocker.
/// The dispatcher must give it the normal executor-failure annotation so it
/// offers recovery, keep the merge-conflict context's state, and not churn or
/// relaunch on later ticks.
#[tokio::test]
async fn dispatcher_blocks_task_whose_failed_execution_was_never_recorded() {
    for status in ["in_progress", "merge_failed"] {
        let db = Arc::new(sqlite_db().await);
        let repo_dir = TempDir::new().expect("repo dir creates");
        let workspace_dir = TempDir::new().expect("workspace dir creates");
        let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
        let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
        let mut task = seed_task(&db, &project_id, "lost failure", status, 0).await;
        task = set_zero_execution_retry_budget(&db, &task).await;
        assign_role(
            &db,
            &task.id,
            crate::workflow::default_roles::CODER,
            &agent_id,
        )
        .await;
        if status == "merge_failed" {
            set_task_error_annotation(
                &db,
                &task,
                r#"{"type":"merge_conflict","blocking_reason":"merge_conflict","recovery_actions":[]}"#,
            )
            .await;
            task = TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .expect("task loads")
                .expect("task exists");
        }
        let execution_id = seed_failed_coder_execution(&db, &task.id, &project_id, &agent_id).await;
        let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

        dispatcher
            .check_once_and_drain()
            .await
            .expect("dispatcher runs");

        let healed = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        let annotation: serde_json::Value =
            serde_json::from_str(healed.error_annotation.as_deref().expect("annotated"))
                .expect("annotation parses");
        assert_eq!(annotation["type"], "executor_failed", "{status}");
        assert_eq!(annotation["blocked_execution_id"], execution_id.as_str());
        assert!(annotation.get("recovery_actions").is_none());
        assert!(dispatcher
            .task_service
            .task_action_offers(
                &healed.id,
                &api_types::Actor::user(api_types::UserActionSource::Test)
            )
            .await
            .unwrap()
            .available_actions
            .iter()
            .any(|offer| offer.action.verb() == "retry"));
        assert!(healed.blocked_json.is_some());
        assert_eq!(healed.status, status);
        assert!(rx.try_recv().is_err(), "no replacement run is launched");

        // Idempotent: a second tick changes nothing.
        dispatcher
            .check_once_and_drain()
            .await
            .expect("dispatcher runs");
        let again = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert_eq!(again.version, healed.version, "{status}");
        assert!(rx.try_recv().is_err());
    }
}

/// The reconciliation must not touch a Task that has a live execution
/// launching or running after the failed one.
#[tokio::test]
async fn dispatcher_leaves_failed_execution_alone_while_a_newer_run_is_live() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "relaunched", "in_progress", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    seed_failed_coder_execution(&db, &task.id, &project_id, &agent_id).await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    seed_running_execution(
        &db,
        &task.id,
        &agent_id,
        crate::workflow::default_roles::CODER,
    )
    .await;
    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    let after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(after.blocked_json.is_none());
    assert!(after.error_annotation.is_none());
}

/// With execution retry budget left, the reconciliation takes the same
/// deferred-retry path as the live failure handler (and only once).
#[tokio::test]
async fn dispatcher_schedules_retry_for_unrecorded_failure_once() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "lost failure", "in_progress", 0).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let execution_id = seed_failed_coder_execution(&db, &task.id, &project_id, &agent_id).await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");
    let scheduled = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(deferred_dispatch::is_pending(
        &scheduled,
        chrono::Utc::now()
    ));
    assert!(scheduled
        .metadata_json
        .as_deref()
        .expect("metadata")
        .contains(&execution_id));

    dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");
    let again = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(again.version, scheduled.version);
    assert!(rx.try_recv().is_err());
}

/// Crash after the retry metadata commits, before the separate execution
/// resume-policy write. Startup must honor that durable retry, not mistake
/// the failed execution's default policy for an owner's manual stop.
#[tokio::test]
async fn startup_recovers_retry_committed_before_resume_policy() {
    for deadline in ["1970-01-01T00:00:00Z", "2099-01-01T00:00:00Z"] {
        let db = Arc::new(sqlite_db().await);
        let repo_dir = TempDir::new().unwrap();
        let workspace_dir = TempDir::new().unwrap();
        let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
        let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
        let task = seed_task(&db, &project_id, "crashed retry", "in_progress", 0).await;
        assign_role(&db, &task.id, "coder", &agent_id).await;
        let execution_id = seed_failed_coder_execution(&db, &task.id, &project_id, &agent_id).await;
        deferred_dispatch::set_with_mutations(
            &db,
            &task,
            "in_progress",
            deadline,
            "usage limit; capacity retry (attempt 1)",
            vec![db::TaskMetadataMutation::Set {
                key: "last_execution_failure_execution_id".into(),
                value: serde_json::json!(execution_id),
            }],
        )
        .await
        .unwrap();
        // No old dispatcher/executor exists: this is exactly the durable prefix
        // left when the process dies at the boundary above.
        let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
        dispatcher.startup_reconcile().await.unwrap();
        for _ in 0..3 {
            dispatcher.check_once_and_drain().await.unwrap();
        }
        if deadline.starts_with("2099") {
            assert!(rx.try_recv().is_err(), "the future deadline still holds");
            let armed = db.schedule_wait_deadline(&task.id).await.unwrap().unwrap();
            assert_eq!(
                chrono::DateTime::parse_from_rfc3339(&armed).unwrap(),
                chrono::DateTime::parse_from_rfc3339(deadline).unwrap()
            );
            let current = TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .unwrap()
                .unwrap();
            deferred_dispatch::set(
                &db,
                &current,
                "in_progress",
                "1970-01-01T00:00:00Z",
                "deadline expired",
            )
            .await
            .unwrap();
            for _ in 0..3 {
                dispatcher.check_once_and_drain().await.unwrap();
            }
        }
        assert_eq!(
            ExecutionRepo::count_by_task_and_role(&*db, &task.id, "coder")
                .await
                .unwrap(),
            2
        );
        let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ctx.task_id, task.id);
        let task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert!(deferred_dispatch::pending_until(&task).is_none());
        assert_eq!(
            ExecutionRepo::count_by_task_and_role(&*db, &task.id, "coder")
                .await
                .unwrap(),
            2
        );
    }
}

/// The startup repair of a retry committed before its policy write must not
/// turn an owner's decision into an automatic retry, and must not charge the
/// retry again. Each database holds the same crash prefix (retry metadata,
/// its budget charge and deadline committed; the failed execution's policy
/// still `manual`), plus one owner decision written afterwards:
/// - Hold: the Task carries the owner's `manual_stop` blocker;
/// - a manual stop: the execution is `cancelled` with `stop_reason`
///   `user_cancelled` (not `failed`);
/// - Cancel: the Task is `cancelled`.
///
/// The repair reads `execution.status = 'failed'`, the Task's
/// `last_execution_failure_execution_id` naming that execution and its
/// `deferred_dispatch.target_state` equal to the Task's status; a held Task
/// is skipped before the repair. None of the three starts a run; the plain
/// prefix starts exactly one and the budget holds exactly one charge.
#[tokio::test]
async fn startup_retry_repair_keeps_owner_decisions_and_charges_once() {
    for decision in ["none", "hold", "manual_stop", "cancel"] {
        let db = Arc::new(sqlite_db().await);
        let repo_dir = TempDir::new().unwrap();
        let workspace_dir = TempDir::new().unwrap();
        let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
        let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
        let task = seed_task(&db, &project_id, "crashed retry", "in_progress", 0).await;
        assign_role(&db, &task.id, "coder", &agent_id).await;
        let execution_id = seed_failed_coder_execution(&db, &task.id, &project_id, &agent_id).await;
        deferred_dispatch::set_with_mutations(
            &db,
            &task,
            "in_progress",
            "1970-01-01T00:00:00Z",
            "execution retry (attempt 1)",
            vec![
                db::TaskMetadataMutation::Budget(db::budget::Mutation::Charge {
                    key: db::budget::Kind::Execution.key().into(),
                    limit: 3,
                    step: format!("failure:{execution_id}"),
                }),
                db::TaskMetadataMutation::Set {
                    key: "last_execution_failure_execution_id".into(),
                    value: serde_json::json!(execution_id),
                },
            ],
        )
        .await
        .unwrap();
        sqlx::query("UPDATE execution SET resume_policy='manual' WHERE id=?")
            .bind(&execution_id)
            .execute(db.pool())
            .await
            .unwrap();
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        match decision {
            "hold" => {
                let at = now_rfc3339();
                TaskRepo::update(
                    &*db,
                    db::UpdateTask {
                        id: current.id.clone(),
                        expected_version: current.version,
                        title: None,
                        description: None,
                        priority: None,
                        merge_config: None,
                        plan: None,
                        error_annotation: Some(Some(
                            serde_json::json!({
                                "type": api_types::FailureKind::ManualStop,
                                "blocking_reason": "held by the owner",
                                "message": "held by the owner",
                                "blocked_at": at,
                            })
                            .to_string(),
                        )),
                        blocked_json: Some(Some(
                            serde_json::json!({
                                "reason": "held by the owner",
                                "created_at": at,
                                "kind": api_types::FailureKind::ManualStop,
                                "source": null,
                                "execution_id": null,
                            })
                            .to_string(),
                        )),
                        failed_json: Some(None),
                        task_state_config: None,
                        parent_task_id: None,
                        updated_at: at,
                    },
                )
                .await
                .unwrap();
            }
            "manual_stop" => {
                sqlx::query(
                    "UPDATE execution SET status='cancelled', stop_reason='user_cancelled', stopped_by='user:api' WHERE id=?",
                )
                .bind(&execution_id)
                .execute(db.pool())
                .await
                .unwrap();
            }
            "cancel" => {
                sqlx::query("UPDATE task SET status='cancelled', version=version+1 WHERE id=?")
                    .bind(&task.id)
                    .execute(db.pool())
                    .await
                    .unwrap();
            }
            _ => {}
        }
        let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
        dispatcher.startup_reconcile().await.unwrap();
        for _ in 0..4 {
            dispatcher.check_once_and_drain().await.unwrap();
        }
        let runs = ExecutionRepo::count_by_task_and_role(&*db, &task.id, "coder")
            .await
            .unwrap();
        let policy = ExecutionRepo::get_by_id(&*db, &execution_id)
            .await
            .unwrap()
            .unwrap()
            .resume_policy;
        if decision == "none" {
            assert_eq!(runs, 2, "the interrupted retry runs once");
            let ctx = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("the retry reaches the executor")
                .expect("context");
            assert_eq!(ctx.task_id, task.id);
        } else {
            assert_eq!(runs, 1, "{decision}: the owner's decision starts nothing");
            assert!(rx.try_recv().is_err(), "{decision}");
            assert_ne!(
                policy,
                Some(ResumePolicy::Auto),
                "{decision}: the repair does not rewrite the stopped run's policy"
            );
        }
        assert_eq!(
            db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
                .await
                .unwrap(),
            1,
            "{decision}: the retry is charged exactly once across the crash"
        );
    }
}

#[tokio::test]
async fn retry_metadata_commits_automatic_resume_policy_atomically() {
    let db = sqlite_db().await;
    let repo_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "atomic retry", "in_progress", 0).await;
    assign_role(&db, &task.id, "coder", &agent_id).await;
    let id = seed_failed_coder_execution(&db, &task.id, &project_id, &agent_id).await;
    let execution = ExecutionRepo::get_by_id(&db, &id).await.unwrap().unwrap();
    let project = ProjectRepo::get_by_id(&db, &project_id)
        .await
        .unwrap()
        .unwrap();
    assert!(deferred_dispatch::set_with_mutations_for_latest_execution(
        &db,
        &task,
        db::LatestExecutionAuthority {
            execution_id: id.clone(),
            role: execution.role.clone(),
            agent_id: execution.agent_id.clone(),
            execution_updated_at: execution.updated_at.clone(),
            expected_project_version: project.version,
        },
        "in_progress",
        "2099-01-01T00:00:00Z",
        "usage limit",
        vec![db::TaskMetadataMutation::Set {
            key: "last_execution_failure_execution_id".into(),
            value: serde_json::json!(id),
        }],
    )
    .await
    .unwrap());
    // No service-level execution update has run: the admission's commit alone
    // must include the policy that makes its deadline actionable.
    let execution = ExecutionRepo::get_by_id(&db, &id).await.unwrap().unwrap();
    assert_eq!(execution.resume_policy, Some(ResumePolicy::Auto));
}
