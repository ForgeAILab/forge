//! Tests for capacity recovery in the task dispatcher.

use super::*;
#[tokio::test]
async fn recovery_on_full_agent_queues_and_dispatches_after_capacity_frees() {
    for (action, bind_execution) in [
        (
            api_types::TaskAction::Retry {
                reason: None,
                fresh_session: Some(true),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            },
            false,
        ),
        (
            api_types::TaskAction::Retry {
                reason: None,
                fresh_session: Some(true),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            },
            true,
        ),
        (
            api_types::TaskAction::Retry {
                reason: None,
                fresh_session: Some(false),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            },
            true,
        ),
    ] {
        let db = Arc::new(sqlite_db().await);
        let repo_dir = TempDir::new().expect("repo dir creates");
        let workspace_dir = TempDir::new().expect("workspace dir creates");
        let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
        let agent_id =
            seed_agent_with_executor(&db, 1, DaemonStatus::Online, AgentStatus::Idle, "codex")
                .await;
        let busy = seed_task(&db, &project_id, "occupies capacity", "in_progress", 0).await;
        assign_role(&db, &busy.id, "coder", &agent_id).await;
        seed_running_execution(&db, &busy.id, &agent_id, "coder").await;
        crate::test_support::set_test_agent_capacity(&db, &agent_id, 1).await;

        let task = seed_task(&db, &project_id, "recover", "in_progress", 0).await;
        assign_role(&db, &task.id, "coder", &agent_id).await;
        let stopped = seed_cancelled_execution(
            &db,
            &task.id,
            &agent_id,
            "coder",
            Some(StopReason::UserCancelled),
            Some(ResumePolicy::Manual),
        )
        .await;
        sqlx::query("UPDATE execution SET agent_session_id = ? WHERE id = ?")
            .bind("recovery-session")
            .bind(&stopped.id)
            .execute(db.pool())
            .await
            .expect("stopped session persists");
        let annotation = serde_json::json!({
            "type": "recovery_required",
            "blocking_reason": "crash_recovery",
            "blocked_execution_id": bind_execution.then_some(&stopped.id),
            "recovery_actions": [action],
        });
        let task = TaskRepo::update(
            &*db,
            UpdateTask {
                id: task.id.clone(),
                expected_version: task.version,
                title: None,
                description: None,
                priority: None,
                merge_config: None,
                plan: None,
                error_annotation: Some(Some(annotation.to_string())),
                blocked_json: Some(Some(
                    serde_json::json!({
                        "kind": "recovery_required",
                        "reason": "crash_recovery",
                    })
                    .to_string(),
                )),
                failed_json: None,
                task_state_config: None,
                parent_task_id: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .expect("interruption persists");
        set_prompt_execution_snapshots(&db, &agent_id).await;
        let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
        let queued = dispatcher
            .task_service
            .test_apply_action(
                &task.id,
                action.clone(),
                Some("operator retry".to_owned()),
                Some("keep this recovery guidance".to_owned()),
            )
            .await
            .expect("capacity-only recovery is accepted");
        assert!(queued.version > task.version);
        assert!(queued.error_annotation.is_none());
        assert!(queued.blocked_json.is_none());
        let intent = deferred_dispatch::queued_recovery(&queued).expect("intent persists");
        let request = &intent.request;
        let mut expected = action.clone();
        if let api_types::TaskAction::Retry {
            reason, guidance, ..
        } = &mut expected
        {
            *reason = Some("operator retry".to_owned());
            *guidance = Some("keep this recovery guidance".to_owned());
        }
        assert_eq!(request.action, expected);
        assert!(
            matches!(&request.action, api_types::TaskAction::Retry { guidance: Some(guidance), .. } if guidance == "keep this recovery guidance")
        );
        let assignments = TaskRoleAssignmentRepo::list_by_task(&*db, &task.id)
            .await
            .expect("assignments load");
        let health = crate::task_diagnostics::derive_workflow_health(
            &queued,
            &crate::workflow::default_workflow::default_workflow(),
            &assignments,
            None,
            None,
            false,
            None,
        );
        assert_eq!(health.kind, api_types::WorkflowHealthKind::WaitingForAgent);
        assert_eq!(health.label, "Retry Queued");
        assert_eq!(
            dispatcher
                .check_once_and_drain()
                .await
                .expect("full scan runs"),
            0
        );
        assert!(rx.try_recv().is_err());
        assert!(ExecutionRepo::list_running_by_task(&*db, &task.id)
            .await
            .expect("executions load")
            .is_empty());

        sqlx::query(
            "UPDATE execution SET status = 'cancelled', resume_policy = 'manual' WHERE task_id = ?",
        )
        .bind(&busy.id)
        .execute(db.pool())
        .await
        .expect("capacity frees");
        // A new dispatcher proves the accepted intent survives runtime restart.
        let (restarted, mut restarted_rx) =
            build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
        assert_eq!(
            restarted
                .check_once_and_drain()
                .await
                .expect("recovery scan runs"),
            1
        );
        let ctx = tokio::time::timeout(Duration::from_secs(30), restarted_rx.recv())
            .await
            .expect("recovery dispatch completes")
            .expect("execution context arrives");
        assert_eq!(ctx.task_id, task.id);
        assert!(ctx.description.contains("keep this recovery guidance"));
        let execution = ExecutionRepo::get_by_id(&*db, &ctx.execution_id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        assert!(execution
            .prompt
            .as_deref()
            .unwrap_or_default()
            .contains("keep this recovery guidance"));
        if action
            == (api_types::TaskAction::Retry {
                reason: None,
                fresh_session: Some(false),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            })
        {
            assert_eq!(
                execution.agent_session_id.as_deref(),
                Some("recovery-session")
            );
            assert_eq!(
                execution.parent_execution_id.as_deref(),
                Some(stopped.id.as_str())
            );
        }
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert!(deferred_dispatch::queued_recovery(&current).is_none());
        assert!(deferred_dispatch::pending_until(&current).is_none());
        assert_eq!(
            restarted
                .check_once_and_drain()
                .await
                .expect("replay scan runs"),
            0
        );
        assert!(restarted_rx.try_recv().is_err());
    }
}

#[tokio::test]
async fn recovery_on_full_agent_restores_blocker_on_permanent_replay_error() {
    for failure in [
        "deleted",
        "unassigned",
        "interactive",
        "paused",
        "offline",
        "unrelated",
        "unrelated_moved",
        "session",
        "unassigned_session",
    ] {
        let db = Arc::new(sqlite_db().await);
        let repo_dir = TempDir::new().unwrap();
        let workspace_dir = TempDir::new().unwrap();
        let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
        let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
        let action = match failure {
            "interactive" => api_types::TaskAction::Retry {
                reason: None,
                fresh_session: Some(false),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            },
            "session" | "unassigned_session" => api_types::TaskAction::Retry {
                reason: None,
                fresh_session: Some(false),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            },
            _ => api_types::TaskAction::Retry {
                reason: None,
                fresh_session: Some(true),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            },
        };
        let (queued, agent_id, busy_id) =
            seed_capacity_recovery(&db, &dispatcher, &project_id, action.clone()).await;
        match failure {
            "deleted" => {
                AgentRepo::archive(&*db, &agent_id, &now_rfc3339())
                    .await
                    .unwrap();
            }
            "unassigned" | "interactive" | "unassigned_session" => {
                sqlx::query("DELETE FROM task_role_assignment WHERE task_id = ?")
                    .bind(&queued.id)
                    .execute(db.pool())
                    .await
                    .unwrap();
            }
            "paused" => {
                sqlx::query(
                    "UPDATE agent_identity SET paused = 1, version = version + 1 WHERE id = ?",
                )
                .bind(&agent_id)
                .execute(db.pool())
                .await
                .unwrap();
            }
            "offline" => {
                let agent = AgentRepo::get_by_id(&*db, &agent_id)
                    .await
                    .unwrap()
                    .unwrap();
                sqlx::query("UPDATE daemon SET status = 'offline' WHERE id = ?")
                    .bind(agent.daemon_id)
                    .execute(db.pool())
                    .await
                    .unwrap();
            }
            "unrelated" | "unrelated_moved" => {
                let other_agent = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
                seed_running_execution(&db, &queued.id, &other_agent, "interactive").await;
                let current = TaskRepo::get_by_id(&*db, &queued.id, false)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    current.metadata_json, queued.metadata_json,
                    "unrelated admission preserves intent"
                );
                if failure == "unrelated_moved" {
                    sqlx::query(
                        "UPDATE task SET status = 'planning', version = version + 1 WHERE id = ?",
                    )
                    .bind(&queued.id)
                    .execute(db.pool())
                    .await
                    .unwrap();
                }
            }
            "session" => {
                sqlx::query("UPDATE execution SET agent_session_id = NULL WHERE task_id = ?")
                    .bind(&queued.id)
                    .execute(db.pool())
                    .await
                    .unwrap();
                sqlx::query("UPDATE execution SET status = 'cancelled', resume_policy = 'manual' WHERE task_id = ?")
                    .bind(&busy_id).execute(db.pool()).await.unwrap();
            }
            _ => unreachable!(),
        }
        let latest = TaskRepo::get_by_id(&*db, &queued.id, false)
            .await
            .unwrap()
            .unwrap();
        if matches!(failure, "paused" | "offline") {
            assert!(!dispatcher
                .task_service
                .dispatch_queued_recovery(&latest)
                .await
                .unwrap());
            let waiting = TaskRepo::get_by_id(&*db, &queued.id, false)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                waiting.version, latest.version,
                "transient waits do not rewrite the Task"
            );
            assert!(deferred_dispatch::queued_recovery(&waiting).is_some());
            assert!(rx.try_recv().is_err());
            continue;
        }
        let error = dispatcher
            .task_service
            .dispatch_queued_recovery(&latest)
            .await
            .expect_err("permanent replay refusal is reported once");
        let restored = TaskRepo::get_by_id(&*db, &queued.id, false)
            .await
            .unwrap()
            .unwrap();
        assert!(restored.version > queued.version);
        assert!(
            deferred_dispatch::queued_recovery(&restored).is_none(),
            "{failure}"
        );
        assert!(deferred_dispatch::pending_until(&restored).is_none());
        let annotation: serde_json::Value =
            serde_json::from_str(restored.error_annotation.as_deref().unwrap()).unwrap();
        assert_eq!(annotation["blocked_by"], "original-owner");
        assert_eq!(annotation["blocking_reason"], error.to_string());
        let blocked: serde_json::Value =
            serde_json::from_str(restored.blocked_json.as_deref().unwrap()).unwrap();
        assert_eq!(blocked["blocked_by"], "original-owner");
        assert_eq!(blocked["reason"], error.to_string());
        let offers = dispatcher
            .task_service
            .task_action_offers(
                &queued.id,
                &api_types::Actor::user(api_types::UserActionSource::Test),
            )
            .await
            .unwrap();
        assert!(offers
            .available_actions
            .iter()
            .any(|offer| offer.action.verb() != "cancel"));
        if failure == "deleted" {
            assert!(!offers
                .available_actions
                .iter()
                .any(|offer| offer.action.verb() == "retry"));
        }
        assert!(!dispatcher
            .task_service
            .dispatch_queued_recovery(&restored)
            .await
            .unwrap());
        assert!(rx.try_recv().is_err());
        if failure == "unassigned" {
            assign_role(&db, &queued.id, "coder", &agent_id).await;
            let requeued = dispatcher
                .task_service
                .test_apply_action(&queued.id, action, None, None)
                .await
                .expect("restored blocker can be recovered after reassignment");
            assert!(deferred_dispatch::queued_recovery(&requeued).is_some());
        }
    }
}

#[tokio::test]
async fn recovery_on_full_agent_resume_fallback_queues_and_replays() {
    for previous_session in [None, Some(false), Some(true)] {
        let db = Arc::new(sqlite_db().await);
        let repo_dir = TempDir::new().unwrap();
        let workspace_dir = TempDir::new().unwrap();
        let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
        let agent_id =
            seed_agent_with_executor(&db, 1, DaemonStatus::Online, AgentStatus::Idle, "codex")
                .await;
        let busy = seed_task(&db, &project_id, "busy", "in_progress", 0).await;
        seed_running_execution(&db, &busy.id, &agent_id, "coder").await;
        crate::test_support::set_test_agent_capacity(&db, &agent_id, 1).await;
        let task = seed_task(&db, &project_id, "resume", "in_progress", 0).await;
        assign_role(&db, &task.id, "coder", &agent_id).await;
        let stopped = if let Some(session) = previous_session {
            let stopped = seed_cancelled_execution(
                &db,
                &task.id,
                &agent_id,
                "coder",
                Some(StopReason::UserCancelled),
                Some(ResumePolicy::Manual),
            )
            .await;
            if session {
                sqlx::query(
                    "UPDATE execution SET agent_session_id = 'resume-session' WHERE id = ?",
                )
                .bind(&stopped.id)
                .execute(db.pool())
                .await
                .unwrap();
            }
            Some(stopped)
        } else {
            None
        };
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        set_prompt_execution_snapshots(&db, &agent_id).await;
        let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
        let queued = dispatcher
            .task_service
            .test_apply_intent(
                &task.id,
                api_types::TaskAction::Retry {
                    reason: Some("resume guidance".to_owned()),
                    fresh_session: None,
                    refresh_workspace: None,
                    reset_budget: None,
                    guidance: Some("resume guidance".to_owned()),
                },
                Some("resume guidance".to_owned()),
                Some(current.version),
            )
            .await
            .expect("Resume queues at capacity")
            .task;
        assert!(deferred_dispatch::queued_recovery(&queued).is_some());
        ProjectRepo::set_paused_at(&*db, &project_id, Some(now_rfc3339()))
            .await
            .unwrap();
        dispatcher.task_service.drain(&task.id).await.unwrap();
        let paused_task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let error = dispatcher
            .task_service
            .perform_task_action(
                &task.id,
                api_types::TaskAction::retry(),
                paused_task.version,
            )
            .await
            .expect_err("the pending intent is not replaced by a second command");
        assert!(matches!(
            error,
            crate::ServiceError::TaskActionUnavailable { .. }
        ));
        assert_eq!(
            TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .unwrap()
                .unwrap(),
            paused_task
        );
        ProjectRepo::set_paused_at(&*db, &project_id, None)
            .await
            .unwrap();
        let queued = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert!(ExecutionRepo::list_running_by_task(&*db, &task.id)
            .await
            .unwrap()
            .is_empty());
        assert!(!dispatcher
            .task_service
            .dispatch_queued_recovery(&queued)
            .await
            .unwrap());
        sqlx::query(
            "UPDATE execution SET status = 'cancelled', resume_policy = 'manual' WHERE task_id = ?",
        )
        .bind(&busy.id)
        .execute(db.pool())
        .await
        .unwrap();
        assert!(dispatcher
            .task_service
            .dispatch_queued_recovery(&queued)
            .await
            .unwrap());
        let ctx = tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .expect("recovery dispatch completes")
            .unwrap();
        assert!(ctx.description.contains("resume guidance"));
        let execution = ExecutionRepo::get_by_id(&*db, &ctx.execution_id)
            .await
            .unwrap()
            .unwrap();
        assert!(execution
            .prompt
            .as_deref()
            .unwrap_or_default()
            .contains("resume guidance"));
        if previous_session == Some(true) {
            assert_eq!(
                execution.parent_execution_id,
                stopped.map(|stopped| stopped.id)
            );
            let snapshot: serde_json::Value =
                serde_json::from_str(execution.executor_config_snapshot_json.as_deref().unwrap())
                    .unwrap();
            assert_eq!(
                snapshot["dispatch"]["execution_policy"],
                "resume_latest_target_role_thread"
            );
        }
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert!(deferred_dispatch::queued_recovery(&current).is_none());
        assert!(deferred_dispatch::pending_until(&current).is_none());
    }
}

#[tokio::test]
async fn recovery_on_full_agent_keeps_non_capacity_refusals() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let busy = seed_task(&db, &project_id, "occupies capacity", "in_progress", 0).await;
    seed_running_execution(&db, &busy.id, &agent_id, "coder").await;
    crate::test_support::set_test_agent_capacity(&db, &agent_id, 1).await;
    let task = seed_task(&db, &project_id, "recover", "in_progress", 0).await;
    assign_role(&db, &task.id, "coder", &agent_id).await;
    let stopped = seed_cancelled_execution(
        &db,
        &task.id,
        &agent_id,
        "coder",
        Some(StopReason::UserCancelled),
        Some(ResumePolicy::Manual),
    )
    .await;
    let annotation = serde_json::json!({
        "type": "recovery_required",
        "blocking_reason": "crash_recovery",
        "blocked_execution_id": stopped.id,
        "recovery_actions": ["resume_session", "reexecute"],
    });
    let task = TaskRepo::update(
        &*db,
        UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(annotation.to_string())),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("interruption persists");
    let (dispatcher, _) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    for (action, _message) in [
        (
            api_types::TaskAction::Retry {
                reason: None,
                fresh_session: Some(false),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            },
            "no resumable session",
        ),
        (
            api_types::TaskAction::Retry {
                reason: None,
                fresh_session: None,
                refresh_workspace: None,
                reset_budget: Some(true),
                guidance: None,
            },
            "not offered",
        ),
    ] {
        let error = dispatcher
            .task_service
            .test_apply_action(&task.id, action, None, None)
            .await
            .expect_err("invalid recovery is refused even at capacity");
        assert!(matches!(
            error,
            crate::ServiceError::TaskActionUnavailable { .. }
        ));
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert_eq!(current.error_annotation, task.error_annotation);
        assert_eq!(current.version, task.version);
        assert!(deferred_dispatch::queued_recovery(&current).is_none());
    }
    sqlx::query("UPDATE agent_identity SET paused = 1, version = version + 1 WHERE id = ?")
        .bind(&agent_id)
        .execute(db.pool())
        .await
        .expect("agent pauses");
    let error = dispatcher
        .task_service
        .test_apply_action(
            &task.id,
            api_types::TaskAction::Retry {
                reason: None,
                fresh_session: Some(true),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            },
            None,
            None,
        )
        .await
        .expect_err("paused agent is refused even at capacity");
    assert!(matches!(
        error,
        crate::ServiceError::TaskActionUnavailable { .. }
    ));
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.error_annotation, task.error_annotation);
    assert!(deferred_dispatch::queued_recovery(&current).is_none());
    sqlx::query("UPDATE agent_identity SET paused = 0, version = version + 1 WHERE id = ?")
        .bind(&agent_id)
        .execute(db.pool())
        .await
        .unwrap();
    let agent = AgentRepo::get_by_id(&*db, &agent_id)
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE daemon SET status = 'offline' WHERE id = ?")
        .bind(agent.daemon_id)
        .execute(db.pool())
        .await
        .unwrap();
    let error = dispatcher
        .task_service
        .test_apply_action(
            &task.id,
            api_types::TaskAction::Retry {
                reason: None,
                fresh_session: Some(true),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            },
            None,
            None,
        )
        .await
        .expect_err("offline agent is refused even at capacity");
    assert!(matches!(
        error,
        crate::ServiceError::TaskActionUnavailable { .. }
    ));
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.error_annotation, task.error_annotation);
    assert!(deferred_dispatch::queued_recovery(&current).is_none());
}
