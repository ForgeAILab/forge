use super::helpers::*;
use super::*;
use api_types::{FailureKind, TaskAction};

async fn fixture(
    db: &SqliteDb,
    project: &str,
    agent: &str,
    state: &str,
    kind: Option<FailureKind>,
) -> Task {
    let task = seed_task_with_status(db, project, state).await;
    seed_role_assignment(db, &task.id, "coder", Some(agent)).await;
    seed_role_assignment(db, &task.id, "planner", Some(agent)).await;
    seed_role_assignment(db, &task.id, "reviewer", Some(agent)).await;
    if let Some(kind) = kind {
        sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?").bind(json!({"type":kind,"blocking_reason":"fixture","recovery_actions":["return_to_implementation","retry_pr_publication"]}).to_string()).bind(&task.id).execute(db.pool()).await.unwrap();
    }
    TaskRepo::get_by_id(db, &task.id, false)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn every_offer_applies_and_every_absent_verb_returns_current_offers() {
    let db = Arc::new(sqlite_db().await);
    let (project, _, repo_dir) = seed_project_repo(&db).await;
    initialize_primary_repository(&repo_dir);
    let agent = seed_agent(&db).await;
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(1024)));
    let verbs = [
        TaskAction::Start,
        TaskAction::Hold { reason: None },
        TaskAction::Release { reason: None },
        TaskAction::retry(),
        TaskAction::SendBack {
            guidance: "Return for revisions.".to_owned(),
        },
        TaskAction::Approve {
            reason: None,
            override_checks: Some(false),
        },
        TaskAction::Restart { reason: None },
        TaskAction::Cancel { reason: None },
    ];
    for state in [
        "backlog",
        "todo",
        "planning",
        "in_progress",
        "review",
        "merging",
        "merge_failed",
        "done",
        "cancelled",
    ] {
        for kind in [
            None,
            Some(FailureKind::ExecutorFailed),
            Some(FailureKind::RetryExhausted),
            Some(FailureKind::ReviewBudgetExhausted),
            Some(FailureKind::MergeFixBudgetExhausted),
            Some(FailureKind::MergeConflict),
            Some(FailureKind::TargetRepoDirty),
            Some(FailureKind::DirtyWorktree),
            Some(FailureKind::CiFailed),
            Some(FailureKind::ReviewGateFailed),
            Some(FailureKind::WorkspaceResetRequired),
            Some(FailureKind::WorkspaceFailed),
            Some(FailureKind::WorkspaceError),
            Some(FailureKind::BeforeWorkHookFailed),
            Some(FailureKind::BeforeWorkHookTimeout),
            Some(FailureKind::WorkflowGuardRejected),
            Some(FailureKind::InternalCommandFailed),
            Some(FailureKind::ReviewBlocked),
            Some(FailureKind::ReviewNeedsOwner),
            Some(FailureKind::MaxTurnsExceeded),
            Some(FailureKind::ManualStop),
            Some(FailureKind::RecoveryRequired),
            Some(FailureKind::ExecutorUnavailable),
            Some(FailureKind::EnvironmentNotReady),
            Some(FailureKind::Unknown),
        ] {
            let task = fixture(&db, &project, &agent, state, kind).await;
            let offers = service
                .task_action_offers(&task.id, &Actor::user(UserActionSource::Test))
                .await
                .unwrap()
                .available_actions;
            for offer in &offers {
                let copy = fixture(&db, &project, &agent, state, kind).await;
                match service
                    .perform_task_action(
                        copy.id.clone(),
                        action_with_operator_inputs(offer),
                        copy.version,
                    )
                    .await
                {
                    Ok(_)
                    | Err(ServiceError::Db(
                        DbError::VersionConflict | DbError::TaskVersionConflict { .. },
                    )) => {}
                    Err(error) => panic!(
                        "offered {} in {state}/{kind:?} failed: {error:?}",
                        offer.action
                    ),
                }
            }
            for action in &verbs {
                if offers
                    .iter()
                    .any(|offer| offer.action.verb() == action.verb())
                {
                    continue;
                }
                let error = service
                    .perform_task_action(task.id.clone(), action.clone(), task.version)
                    .await
                    .unwrap_err();
                match error {
                    ServiceError::TaskActionUnavailable {
                        available_actions, ..
                    } => assert_eq!(available_actions, offers),
                    error => panic!("absent {} returned {error:?}", action.verb()),
                }
            }
        }
    }
}

#[tokio::test]
async fn unauthorized_caller_gets_no_offers_and_one_typed_error_for_each_verb() {
    let db = Arc::new(sqlite_db().await);
    let (project, _, _repo_dir) = seed_project_repo(&db).await;
    let agent = seed_agent(&db).await;
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(32)));
    let task = fixture(
        &db,
        &project,
        &agent,
        "in_progress",
        Some(FailureKind::ExecutorFailed),
    )
    .await;
    let actor = Actor::agent("unassigned");
    assert!(service
        .task_action_offers(&task.id, &actor)
        .await
        .unwrap()
        .available_actions
        .is_empty());
    for action in [
        TaskAction::Start,
        TaskAction::Hold { reason: None },
        TaskAction::Release { reason: None },
        TaskAction::retry(),
        TaskAction::SendBack {
            guidance: "revision".to_owned(),
        },
        TaskAction::Approve {
            reason: None,
            override_checks: Some(true),
        },
        TaskAction::Restart { reason: None },
        TaskAction::Cancel { reason: None },
    ] {
        let error = service
            .perform_task_action_as(task.id.clone(), action, task.version, actor.clone())
            .await
            .unwrap_err();
        assert!(
            matches!(error, ServiceError::TaskActionUnavailable { available_actions, .. } if available_actions.is_empty())
        );
    }
}

#[tokio::test]
async fn stale_version_is_a_version_conflict_before_any_action_effect() {
    let db = Arc::new(sqlite_db().await);
    let (project, _, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project, "in_progress").await;
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(16)));
    assert!(matches!(
        service
            .perform_task_action(
                &task.id,
                TaskAction::Cancel { reason: None },
                task.version + 1
            )
            .await,
        Err(ServiceError::Db(DbError::TaskVersionConflict { .. }))
    ));
    assert_eq!(
        TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap(),
        task
    );
}

struct RecordingExecutor {
    started: tokio::sync::mpsc::UnboundedSender<String>,
}
#[async_trait::async_trait]
impl TaskExecutor for RecordingExecutor {
    async fn execute(
        &self,
        context: ExecutionContext,
    ) -> std::result::Result<executors::ExecutionResult, executors::ExecutorError> {
        self.started.send(context.execution_id.clone()).unwrap();
        std::future::pending().await
    }
    async fn cancel(
        &self,
        _execution_id: &str,
    ) -> std::result::Result<(), executors::ExecutorError> {
        Ok(())
    }
}

#[tokio::test]
async fn recovery_commits_at_capacity_then_dispatches_when_the_slot_frees() {
    let db = Arc::new(sqlite_db().await);
    let (project, _, repo_dir) = seed_project_repo(&db).await;
    initialize_primary_repository(&repo_dir);
    let agent = seed_agent(&db).await;
    let busy = seed_task_with_status(&db, &project, "in_progress").await;
    let occupied = seed_execution(
        &db,
        &busy.id,
        Some(&agent),
        "coder",
        ExecutionStatus::Running,
        None,
        "2026-10-02T00:00:00Z",
    )
    .await;
    let agent_row = db::AgentRepo::get_by_id(&*db, &agent)
        .await
        .unwrap()
        .unwrap();
    AgentRepo::update(
        &*db,
        db::UpdateAgent {
            id: agent_row.id,
            expected_version: agent_row.version,
            name: None,
            description: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: None,
            config_json: None,
            daemon_id: Some(None),
            max_concurrent_tasks: None,
            heartbeat_interval_seconds: None,
            max_missed_heartbeats: None,
            status: None,
            last_heartbeat_at: None,
            is_default: None,
            paused: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(128)))
        .with_workspace_root(repo_dir.path().join("workspaces"))
        .with_task_executor(Arc::new(RecordingExecutor { started }));
    for state in ["planning", "in_progress", "merge_failed"] {
        let task = fixture(
            &db,
            &project,
            &agent,
            state,
            Some(FailureKind::ExecutorFailed),
        )
        .await;
        let accepted = service
            .perform_task_action(&task.id, TaskAction::retry(), task.version)
            .await
            .unwrap()
            .task;
        assert!(accepted.error_annotation.is_none());
        assert!(crate::deferred_dispatch::queued_recovery(&accepted).is_some());
        assert!(!service.dispatch_queued_recovery(&accepted).await.unwrap());
        assert!(starts.try_recv().is_err());
        // Free the unrelated Agent slot; the accepted intent, not another command, runs next.
        sqlx::query("UPDATE execution SET status = 'cancelled' WHERE id = ?")
            .bind(&occupied.id)
            .execute(db.pool())
            .await
            .unwrap();
        assert!(service.dispatch_queued_recovery(&accepted).await.unwrap());
        let execution_id = tokio::time::timeout(Duration::from_secs(5), starts.recv())
            .await
            .unwrap()
            .unwrap();
        let execution = ExecutionRepo::get_by_id(&*db, &execution_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(execution.task_id, task.id);
        assert!(crate::deferred_dispatch::queued_recovery(
            &TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .unwrap()
                .unwrap()
        )
        .is_none());
        sqlx::query("UPDATE execution SET status = 'cancelled' WHERE id = ?")
            .bind(&execution_id)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE execution SET status = 'running' WHERE id = ?")
            .bind(&occupied.id)
            .execute(db.pool())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn send_back_commits_at_capacity_and_dispatcher_resumes_the_worker_thread() {
    let db = Arc::new(sqlite_db().await);
    let (project, _, repo_dir) = seed_project_repo(&db).await;
    initialize_primary_repository(&repo_dir);
    let workflow = crate::workflow::default_autonomous_workflow::default_autonomous_workflow();
    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(serde_json::to_string(&workflow).unwrap())
        .bind(&project)
        .execute(db.pool())
        .await
        .unwrap();
    let agent = seed_agent(&db).await;
    let row = AgentRepo::get_by_id(&*db, &agent).await.unwrap().unwrap();
    AgentRepo::update(
        &*db,
        db::UpdateAgent {
            id: row.id,
            expected_version: row.version,
            name: None,
            description: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: None,
            config_json: None,
            daemon_id: Some(None),
            max_concurrent_tasks: None,
            heartbeat_interval_seconds: None,
            max_missed_heartbeats: None,
            status: None,
            last_heartbeat_at: None,
            is_default: None,
            paused: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let task = seed_task_with_status(&db, &project, "review").await;
    seed_role_assignment(&db, &task.id, "worker", Some(&agent)).await;
    let worker = seed_execution(
        &db,
        &task.id,
        Some(&agent),
        "worker",
        ExecutionStatus::Completed,
        Some("existing-worker-thread"),
        "2026-10-02T00:00:00Z",
    )
    .await;
    let busy = seed_task_with_status(&db, &project, "working").await;
    let occupied = seed_execution(
        &db,
        &busy.id,
        Some(&agent),
        "worker",
        ExecutionStatus::Running,
        None,
        "2026-10-02T00:00:01Z",
    )
    .await;
    let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
    let service = TaskService::new(db.clone(), Arc::new(EventBus::new(128)))
        .with_workspace_root(repo_dir.path().join("workspaces"))
        .with_task_executor(Arc::new(RecordingExecutor { started }));
    let accepted = service
        .perform_task_action(
            &task.id,
            TaskAction::SendBack {
                guidance: "Add evidence".to_owned(),
            },
            task.version,
        )
        .await
        .unwrap()
        .task;
    assert_eq!(accepted.status, "working");
    let queued =
        crate::deferred_dispatch::queued_recovery(&accepted).expect("worker continuation queued");
    assert_eq!(
        queued.request.offer.target_execution_id.as_deref(),
        Some(worker.id.as_str())
    );
    assert!(!service.dispatch_queued_recovery(&accepted).await.unwrap());
    assert!(starts.try_recv().is_err());
    sqlx::query("UPDATE execution SET status = 'cancelled' WHERE id = ?")
        .bind(&occupied.id)
        .execute(db.pool())
        .await
        .unwrap();
    assert!(service.dispatch_queued_recovery(&accepted).await.unwrap());
    let execution_id = tokio::time::timeout(Duration::from_secs(5), starts.recv())
        .await
        .unwrap()
        .unwrap();
    let execution = ExecutionRepo::get_by_id(&*db, &execution_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(execution.role, "worker");
    assert_eq!(
        execution.parent_execution_id.as_deref(),
        Some(worker.id.as_str())
    );
}

async fn special_fixture(db: &SqliteDb, project: &str, agent: &str, scenario: &str) -> Task {
    let state = match scenario {
        "coordination_root" => "todo",
        "failed_review" | "flow_park" | "entry_barrier" => "review",
        "conflict_handoff" => "merge_failed",
        _ => "in_progress",
    };
    let kind = match scenario {
        "entry_barrier" => Some(FailureKind::BeforeWorkHookFailed),
        "flow_park" => Some(FailureKind::ReviewNeedsOwner),
        "conflict_handoff" => Some(FailureKind::MergeConflict),
        _ => None,
    };
    let task = fixture(db, project, agent, state, kind).await;
    match scenario {
        "failed_review" | "flow_park" => {
            let execution = seed_execution(
                db,
                &task.id,
                Some(agent),
                "coder",
                ExecutionStatus::Completed,
                Some("candidate"),
                "2026-10-02T00:00:00Z",
            )
            .await;
            seed_failed_review(
                db,
                &task.id,
                &execution.id,
                1,
                json!({"ci_steps":[{"index":0,"command":"check","exit_code":1,"stderr_tail":""}]}),
            )
            .await;
        }
        "entry_barrier" => {
            sqlx::query("UPDATE task SET entry_barrier_json = ? WHERE id = ?")
                .bind(json!({"state":state,"status":"blocked","hook_results":[]}).to_string())
                .bind(&task.id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        "hard_failure" => {
            sqlx::query("UPDATE task SET failed_json = ? WHERE id = ?")
                .bind(json!({"kind":"executor_failed","reason":"failed fixture"}).to_string())
                .bind(&task.id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        "legacy_blocked" => {
            sqlx::query("UPDATE task SET blocked_json = ? WHERE id = ?").bind(json!({"kind":"retry_exhausted","reason":"spent","recovery_actions":["return_to_implementation","retry_pr_publication"]}).to_string()).bind(&task.id).execute(db.pool()).await.unwrap();
        }
        "coordination_root" => {
            let child = seed_task_with_status(db, project, "todo").await;
            sqlx::query("UPDATE task SET parent_task_id = ? WHERE id = ?")
                .bind(&task.id)
                .bind(&child.id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        "conflict_handoff" => {
            sqlx::query("UPDATE task SET description = '[conflict-handoff] repair the committed markers' WHERE id = ?").bind(&task.id).execute(db.pool()).await.unwrap();
        }
        "interactive_session" => {
            seed_execution(
                db,
                &task.id,
                None,
                "interactive",
                ExecutionStatus::Running,
                None,
                "2026-10-02T00:00:00Z",
            )
            .await;
        }
        "running_role" => {
            let runner = seed_agent(db).await;
            seed_role_assignment(db, &task.id, "coder", Some(&runner)).await;
            seed_execution(
                db,
                &task.id,
                Some(&runner),
                "coder",
                ExecutionStatus::Running,
                None,
                "2026-10-02T00:00:00Z",
            )
            .await;
        }
        _ => {}
    }
    TaskRepo::get_by_id(db, &task.id, false)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn special_snapshot_offers_apply_or_version_conflict_and_absent_verbs_are_unavailable() {
    let db = Arc::new(sqlite_db().await);
    let (project, _, repo_dir) = seed_project_repo(&db).await;
    initialize_primary_repository(&repo_dir);
    let agent = seed_agent(&db).await;
    // Agent capacity is full throughout; it does not alter offers or acceptance.
    let busy = seed_task_with_status(&db, &project, "in_progress").await;
    seed_execution(
        &db,
        &busy.id,
        Some(&agent),
        "coder",
        ExecutionStatus::Running,
        None,
        "2026-10-02T00:00:00Z",
    )
    .await;
    let service = TaskService::new(db.clone(), Arc::new(EventBus::new(1024)));
    let verbs = [
        TaskAction::Start,
        TaskAction::Hold { reason: None },
        TaskAction::Release { reason: None },
        TaskAction::retry(),
        TaskAction::SendBack {
            guidance: "Revise".to_owned(),
        },
        TaskAction::Approve {
            reason: None,
            override_checks: Some(false),
        },
        TaskAction::Restart { reason: None },
        TaskAction::Cancel { reason: None },
    ];
    for scenario in [
        "failed_review",
        "flow_park",
        "entry_barrier",
        "hard_failure",
        "legacy_blocked",
        "coordination_root",
        "conflict_handoff",
        "running_role",
        "interactive_session",
    ] {
        let task = special_fixture(&db, &project, &agent, scenario).await;
        let offers = service
            .task_action_offers(&task.id, &Actor::user(UserActionSource::Test))
            .await
            .unwrap()
            .available_actions;
        for offer in &offers {
            let copy = special_fixture(&db, &project, &agent, scenario).await;
            match service
                .perform_task_action(&copy.id, action_with_operator_inputs(offer), copy.version)
                .await
            {
                Ok(_)
                | Err(ServiceError::Db(
                    DbError::VersionConflict | DbError::TaskVersionConflict { .. },
                )) => {}
                Err(error) => panic!("{} offered in {scenario} failed: {error:?}", offer.action),
            }
        }
        for action in &verbs {
            if offers
                .iter()
                .any(|offer| offer.action.verb() == action.verb())
            {
                continue;
            }
            match service
                .perform_task_action(&task.id, action.clone(), task.version)
                .await
                .unwrap_err()
            {
                ServiceError::TaskActionUnavailable {
                    available_actions, ..
                } => assert_eq!(available_actions, offers),
                error => panic!("absent {} in {scenario} returned {error:?}", action.verb()),
            }
        }
    }
}

#[tokio::test]
async fn historical_queued_commands_preserve_payload_and_current_conditions() {
    let db = Arc::new(sqlite_db().await);
    let (project, _, _repo) = seed_project_repo(&db).await;
    let agent = seed_agent(&db).await;
    let service = TaskService::new(db.clone(), Arc::new(EventBus::new(64)));
    for same_state in [true, false] {
        let task = fixture(&db, &project, &agent, "in_progress", None).await;
        let old = json!({"id":"old-intent","target_state":if same_state { "in_progress" } else { "review" },
            "request":{"action":"open_interactive","reason":"inspect the finding"},
            "error_annotation":json!({"type":"executor_failed","blocking_reason":"old condition","recovery_actions":["return_to_implementation","retry_pr_publication"]}).to_string(),"blocked_json":null});
        let current =
            json!({"type":"workspace_error","blocking_reason":"new condition"}).to_string();
        sqlx::query("UPDATE task SET metadata_json = ?, error_annotation = ? WHERE id = ?")
            .bind(json!({"queued_recovery":old}).to_string())
            .bind(&current)
            .bind(&task.id)
            .execute(db.pool())
            .await
            .unwrap();
        let task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert!(
            service.dispatch_queued_recovery(&task).await.is_err(),
            "unrepresentable legacy intents settle with an error"
        );
        let current_task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            current_task.error_annotation.as_deref(),
            Some(current.as_str())
        );
        let metadata: Value =
            serde_json::from_str(current_task.metadata_json.as_deref().unwrap()).unwrap();
        assert_eq!(metadata["task_action_migrated_intent"], old);
        assert!(metadata.get("queued_recovery").is_none());
    }
}

#[tokio::test]
async fn owner_advance_override_stops_the_role_and_dispatches_the_next_without_a_plan() {
    let db = Arc::new(sqlite_db().await);
    let (project, _, repo) = seed_project_repo(&db).await;
    initialize_primary_repository(&repo);
    let agent = super::condition_scenarios::scenario_agent(&db).await;
    let task = fixture(&db, &project, &agent, "planning", None).await;
    seed_role_assignment(&db, &task.id, "planner", Some(&agent)).await;
    let running = seed_execution(
        &db,
        &task.id,
        Some(&agent),
        "planner",
        ExecutionStatus::Running,
        None,
        "2026-10-02T00:00:00Z",
    )
    .await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let service = TaskService::new(db.clone(), Arc::new(EventBus::new(64)))
        .with_workspace_root(repo.path().join("workspaces"))
        .with_task_executor(Arc::new(RecordingExecutor { started: tx }));
    let result = service
        .perform_task_action(
            &task.id,
            TaskAction::Approve {
                override_checks: Some(true),
                reason: Some("Owner supplies an alternate plan".to_owned()),
            },
            task.version,
        )
        .await
        .unwrap();
    assert_eq!(result.task.status, "in_progress");
    assert_eq!(
        ExecutionRepo::get_by_id(&*db, &running.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ExecutionStatus::Cancelled
    );
    assert!(ExecutionRepo::list_running_by_task(&*db, &task.id)
        .await
        .unwrap()
        .is_empty());
    assert!(
        rx.try_recv().is_err(),
        "the command cannot launch the next role"
    );
    service.test_dispatch_task_action(&task.id).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let rows = ExecutionRepo::list_running_by_task(&*db, &task.id)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].role, "coder");
    assert!(TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap()
        .iter()
        .any(|row| row.trigger_reason == "Owner supplies an alternate plan" && !row.rejection));
}

#[tokio::test]
async fn stale_role_is_restored_before_a_paused_agent_wait() {
    let db = Arc::new(sqlite_db().await);
    let (project, _, _) = seed_project_repo(&db).await;
    let agent = seed_agent(&db).await;
    let task = fixture(
        &db,
        &project,
        &agent,
        "in_progress",
        Some(FailureKind::ExecutorFailed),
    )
    .await;
    let service = TaskService::new(db.clone(), Arc::new(EventBus::new(64)));
    let queued = service
        .perform_task_action(&task.id, TaskAction::retry(), task.version)
        .await
        .unwrap()
        .task;
    sqlx::query("DELETE FROM task_role_assignment WHERE task_id = ? AND role_name = 'coder'")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE agent_identity SET paused = 1 WHERE id = ?")
        .bind(&agent)
        .execute(db.pool())
        .await
        .unwrap();
    assert!(service.dispatch_queued_recovery(&queued).await.is_err());
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(crate::deferred_dispatch::queued_recovery(&current).is_none());
    assert!(current
        .error_annotation
        .as_deref()
        .unwrap()
        .contains("ownership changed"));
}

#[tokio::test]
async fn hold_stops_workflow_execution_and_records_operator_reason() {
    let db = Arc::new(sqlite_db().await);
    let (project, _, _) = seed_project_repo(&db).await;
    let agent = seed_agent(&db).await;
    let task = fixture(&db, &project, &agent, "in_progress", None).await;
    let execution = seed_execution(
        &db,
        &task.id,
        Some(&agent),
        "coder",
        ExecutionStatus::Running,
        Some("held-session"),
        "2026-10-02T00:00:00Z",
    )
    .await;
    let service = TaskService::new(db.clone(), Arc::new(EventBus::new(64)));
    let held = service
        .perform_task_action(
            &task.id,
            TaskAction::Hold {
                reason: Some("Wait for the owner measurements".to_owned()),
            },
            task.version,
        )
        .await
        .unwrap()
        .task;
    assert_eq!(held.status, "in_progress");
    let stopped = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stopped.status, ExecutionStatus::Cancelled);
    let comments = db::TaskCommentRepo::list_comments(
        &*db,
        &task.id,
        db::PageRequest {
            cursor: None,
            limit: 100,
            include_total: false,
            sort_by: db::SortBy::CreatedAt,
            sort_order: db::SortOrder::Asc,
        },
    )
    .await
    .unwrap();
    assert!(comments.items.iter().any(|comment| {
        comment.author_type == db::CommentAuthorType::System
            && comment.content == "Task paused by user: Wait for the owner measurements"
    }));
    let offers = service
        .task_action_offers(&task.id, &Actor::user(UserActionSource::Test))
        .await
        .unwrap();
    assert!(offers
        .available_actions
        .iter()
        .any(|offer| matches!(offer.action, TaskAction::Release { .. })));
}

#[tokio::test]
async fn retry_fixed_presets_are_applied_and_contradictions_refused() {
    let db = Arc::new(sqlite_db().await);
    let (project, _, _) = seed_project_repo(&db).await;
    let agent = seed_agent(&db).await;
    let task = fixture(
        &db,
        &project,
        &agent,
        "in_progress",
        Some(FailureKind::ExecutorFailed),
    )
    .await;
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(32)));
    let offers = service
        .task_action_offers(&task.id, &Actor::user(UserActionSource::Test))
        .await
        .unwrap();
    assert!(offers.available_actions.iter().any(|offer| matches!(
        offer.action,
        TaskAction::Retry {
            fresh_session: Some(true),
            ..
        }
    )));
    let contrary = TaskAction::Retry {
        reason: None,
        fresh_session: Some(false),
        refresh_workspace: None,
        reset_budget: None,
        guidance: None,
    };
    assert!(matches!(
        service
            .perform_task_action(&task.id, contrary, task.version)
            .await,
        Err(ServiceError::TaskActionUnavailable { .. })
    ));
    let applied = service
        .perform_task_action(&task.id, TaskAction::retry(), task.version)
        .await
        .unwrap();
    assert!(matches!(
        applied.action,
        TaskAction::Retry {
            fresh_session: Some(true),
            ..
        }
    ));
}

#[tokio::test]
async fn owner_reconciliation_keeps_the_merge_retry_offer_reason() {
    let db = Arc::new(sqlite_db().await);
    let (task, placement, _) = crate::recovery::tests::daemon_owned_fixture(&db).await;
    sqlx::query("UPDATE task SET status = 'merging' WHERE id = ?")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
    let registry = Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
    let daemon_id = placement.daemon_id.clone().unwrap();
    let (connection_id, mut outbound) =
        crate::recovery::tests::owner_connection(&registry, &daemon_id, false);
    let responder = {
        let registry = Arc::clone(&registry);
        tokio::spawn(async move {
            let api_types::DaemonFrame::Request { id, method, params } =
                outbound.recv().await.unwrap()
            else {
                panic!("owner describe request")
            };
            assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
            registry.dispatch_incoming_for_connection(&daemon_id, connection_id, api_types::DaemonFrame::Response {
                id, result: json!({"workspace_handle":params["workspace_handle"],"generation":1,"exists":true,"head_sha":"base-head","dirty":false,"branch":"task/remote","locked":false,"active_execution_ids":[],"journaled_execution_ids":[]}),
            });
        })
    };
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(32)))
        .with_daemon_connections(registry);
    let result = service
        .perform_task_action(&task.id, TaskAction::retry(), task.version)
        .await
        .unwrap();
    responder.await.unwrap();
    let queued = crate::deferred_dispatch::queued_recovery(&result.task)
        .expect("reconciled merge retry queues");
    assert_eq!(queued.request.offer.reason, "merge_gate_retry");
}
