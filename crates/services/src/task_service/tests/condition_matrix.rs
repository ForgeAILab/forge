//! Condition-only reproduction matrix: command commit + dispatcher consumption.
use super::helpers::*;
use super::*;
use api_types::{FailureKind, TaskAction};
use std::fmt::Write as _;

struct ConditionPendingExecutor;
#[async_trait::async_trait]
impl TaskExecutor for ConditionPendingExecutor {
    async fn execute(
        &self,
        _context: ExecutionContext,
    ) -> std::result::Result<executors::ExecutionResult, executors::ExecutorError> {
        std::future::pending().await
    }
    async fn cancel(
        &self,
        _execution_id: &str,
    ) -> std::result::Result<(), executors::ExecutorError> {
        Ok(())
    }
}

fn condition_role_for(workflow: &str, state: &str) -> Option<&'static str> {
    match (workflow, state) {
        ("std", "planning") => Some("planner"),
        ("std", "in_progress" | "merge_failed") => Some("coder"),
        ("std", "review") => Some("reviewer"),
        ("auto", "working" | "merge_failed") => Some("worker"),
        _ => None,
    }
}

async fn condition_fixture(
    db: &SqliteDb,
    project: &str,
    agent: &str,
    workflow: &str,
    state: &str,
    kind: Option<FailureKind>,
    variant: &str,
) -> Task {
    let task = seed_task_with_status(db, project, state).await;
    if workflow == "std" {
        seed_role_assignment(db, &task.id, "coder", Some(agent)).await;
        seed_role_assignment(db, &task.id, "planner", Some(agent)).await;
        seed_role_assignment(db, &task.id, "reviewer", Some(agent)).await;
    } else {
        seed_role_assignment(db, &task.id, "worker", Some(agent)).await;
    }
    if let Some(kind) = kind {
        sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
            .bind(json!({"type":kind,"blocking_reason":"fixture","recovery_actions":["retry_hook","return_to_implementation"]}).to_string())
            .bind(&task.id)
            .execute(db.pool())
            .await
            .unwrap();
    }
    let work_role = if workflow == "std" { "coder" } else { "worker" };
    let reject_target = if workflow == "std" {
        "in_progress"
    } else {
        "working"
    };
    match variant {
        "role_exec_done" => {
            if let Some(role) = condition_role_for(workflow, state) {
                seed_execution(
                    db,
                    &task.id,
                    Some(agent),
                    role,
                    ExecutionStatus::Completed,
                    Some("thread"),
                    "2026-10-02T00:00:00Z",
                )
                .await;
            } else {
                seed_execution(
                    db,
                    &task.id,
                    Some(agent),
                    work_role,
                    ExecutionStatus::Completed,
                    Some("thread"),
                    "2026-10-02T00:00:00Z",
                )
                .await;
            }
        }
        "review_failed" | "review_awaiting" | "review_running_orphan" | "review_passed" => {
            let execution = seed_execution(
                db,
                &task.id,
                Some(agent),
                work_role,
                ExecutionStatus::Completed,
                Some("candidate"),
                "2026-10-02T00:00:00Z",
            )
            .await;
            let status = match variant {
                "review_failed" => ReviewStatus::Failed,
                "review_awaiting" => ReviewStatus::AwaitingHuman,
                "review_passed" => ReviewStatus::Passed,
                _ => ReviewStatus::Running,
            };
            let now = now_rfc3339();
            db::ReviewRepo::create(
                db,
                db::CreateReview {
                    id: new_uuid_v4(),
                    task_id: task.id.clone(),
                    execution_id: execution.id.clone(),
                    attempt_number: 1,
                    status,
                    step_results_json: json!({"ci_steps":[]}).to_string(),
                    started_at: now.clone(),
                    created_at: now.clone(),
                    updated_at: now,
                },
            )
            .await
            .unwrap();
        }
        "rejections_max" => {
            seed_execution(
                db,
                &task.id,
                Some(agent),
                work_role,
                ExecutionStatus::Completed,
                Some("candidate"),
                "2026-10-02T00:00:00Z",
            )
            .await;
            for _ in 0..3 {
                db::TransitionLogRepo::insert(
                    db,
                    db::CreateTransitionLog {
                        id: new_uuid_v4(),
                        task_id: task.id.clone(),
                        from_state: "review".to_owned(),
                        to_state: reject_target.to_owned(),
                        trigger_name: Some("reject".to_owned()),
                        triggered_by: "user".to_owned(),
                        trigger_reason: "fixture rejection".to_owned(),
                        hook_results_json: None,
                        rejection: true,
                        created_at: now_rfc3339(),
                    },
                )
                .await
                .unwrap();
            }
        }
        "failed_json" => {
            sqlx::query("UPDATE task SET failed_json = ? WHERE id = ?")
                .bind(json!({"kind":"executor_failed","reason":"failed fixture"}).to_string())
                .bind(&task.id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        "barrier_blocked" => {
            sqlx::query("UPDATE task SET entry_barrier_json = ? WHERE id = ?")
                .bind(json!({"state":state,"status":"blocked","hook_results":[]}).to_string())
                .bind(&task.id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        "blocked_retry_exhausted" => {
            sqlx::query("UPDATE task SET blocked_json = ? WHERE id = ?")
                .bind(
                    json!({"kind":"retry_exhausted","reason":"spent","created_at":now_rfc3339()})
                        .to_string(),
                )
                .bind(&task.id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        _ => {}
    }
    TaskRepo::get_by_id(db, &task.id, false)
        .await
        .unwrap()
        .unwrap()
}

fn condition_short(error: &ServiceError) -> String {
    let mut text = format!("{error}");
    text.retain(|character| character != '\n' && character != '\t');
    // strip uuids so identical failures aggregate
    let mut out = String::new();
    for word in text.split(' ') {
        if word.len() >= 32 && word.chars().filter(|character| *character == '-').count() >= 4 {
            out.push_str("<id> ");
        } else {
            out.push_str(word);
            out.push(' ');
        }
    }
    out.chars().take(160).collect()
}

fn condition_marker_present(task: &Task) -> bool {
    task.metadata_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .is_some_and(|metadata| metadata.get("queued_recovery").is_some())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn condition_offer_apply_dispatch_matrix() {
    let db = Arc::new(sqlite_db().await);
    let (mut snapshots, mut accepted, mut applied, mut restored, mut dead_ends, mut command_errors) =
        (0usize, 0usize, 0usize, 0usize, 0usize, 0usize);
    let mut out = String::new();
    let kinds: Vec<Option<FailureKind>> = vec![
        None,
        Some(FailureKind::MergeConflict),
        Some(FailureKind::TargetRepoDirty),
        Some(FailureKind::DirtyWorktree),
        Some(FailureKind::CiFailed),
        Some(FailureKind::ReviewGateFailed),
        Some(FailureKind::ReviewBudgetExhausted),
        Some(FailureKind::ReviewBlocked),
        Some(FailureKind::ReviewNeedsOwner),
        Some(FailureKind::EnvironmentNotReady),
        Some(FailureKind::RetryExhausted),
        Some(FailureKind::MergeFixBudgetExhausted),
        Some(FailureKind::WorkflowGuardRejected),
        Some(FailureKind::InternalCommandFailed),
        Some(FailureKind::ExecutorFailed),
        Some(FailureKind::WorkspaceFailed),
        Some(FailureKind::WorkspaceResetRequired),
        Some(FailureKind::WorkspaceError),
        Some(FailureKind::BeforeWorkHookTimeout),
        Some(FailureKind::BeforeWorkHookFailed),
        Some(FailureKind::MaxTurnsExceeded),
        Some(FailureKind::ManualStop),
        Some(FailureKind::RecoveryRequired),
        Some(FailureKind::ExecutorUnavailable),
        Some(FailureKind::Unknown),
        Some(FailureKind::DispatchFailed),
        Some(FailureKind::DependencyCancelled),
    ];
    let all_verbs = [
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
    for workflow in ["std", "auto"] {
        let (project, _, repo_dir) = seed_project_repo(&db).await;
        initialize_primary_repository(&repo_dir);
        // Settlement matrix isolates permanent outcomes; capacity/pause waits
        // are covered separately by the scenario tests.
        sqlx::query("UPDATE project SET settings = json_set(settings, '$.max_active_tasks', 0) WHERE id = ?")
            .bind(&project).execute(db.pool()).await.unwrap();
        let states: Vec<&str> = if workflow == "std" {
            vec![
                "backlog",
                "todo",
                "planning",
                "in_progress",
                "review",
                "merging",
                "merge_failed",
            ]
        } else {
            let definition =
                crate::workflow::default_autonomous_workflow::default_autonomous_workflow();
            sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
                .bind(serde_json::to_string(&definition).unwrap())
                .bind(&project)
                .execute(db.pool())
                .await
                .unwrap();
            vec![
                "backlog",
                "ready",
                "working",
                "review",
                "merging",
                "merge_failed",
            ]
        };
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
        let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(4096)))
            .with_workspace_root(repo_dir.path().join("workspaces"))
            .with_task_executor(Arc::new(ConditionPendingExecutor));
        for state in states {
            let variants: Vec<&str> = match state {
                "review" => vec![
                    "bare",
                    "role_exec_done",
                    "review_failed",
                    "review_awaiting",
                    "review_running_orphan",
                    "review_passed",
                    "rejections_max",
                    "failed_json",
                    "barrier_blocked",
                    "blocked_retry_exhausted",
                ],
                "backlog" => vec!["bare", "failed_json"],
                _ => vec![
                    "bare",
                    "role_exec_done",
                    "rejections_max",
                    "failed_json",
                    "barrier_blocked",
                    "blocked_retry_exhausted",
                ],
            };
            for variant in variants {
                let variant_kinds: Vec<Option<FailureKind>> = match variant {
                    "bare" | "role_exec_done" => kinds.clone(),
                    "review_failed" => vec![
                        None,
                        Some(FailureKind::ReviewBlocked),
                        Some(FailureKind::ReviewNeedsOwner),
                        Some(FailureKind::ReviewBudgetExhausted),
                        Some(FailureKind::ReviewGateFailed),
                        Some(FailureKind::ExecutorFailed),
                    ],
                    "barrier_blocked" => vec![None, Some(FailureKind::BeforeWorkHookFailed)],
                    "rejections_max" => vec![
                        None,
                        Some(FailureKind::ReviewBudgetExhausted),
                        Some(FailureKind::ExecutorFailed),
                    ],
                    _ => vec![None],
                };
                for kind in variant_kinds {
                    snapshots += 1;
                    let label = format!(
                        "{workflow}|{state}|{variant}|{}",
                        kind.map(|kind| kind.to_string())
                            .unwrap_or_else(|| "none".to_owned())
                    );
                    let task =
                        condition_fixture(&db, &project, &agent, workflow, state, kind, variant)
                            .await;
                    let offers = match service
                        .task_action_offers(&task.id, &Actor::user(UserActionSource::Test))
                        .await
                    {
                        Ok(response) => response.available_actions,
                        Err(error) => {
                            let _ = writeln!(
                                out,
                                "CONDITION|OFFERS_ERR|{label}|{}",
                                condition_short(&error)
                            );
                            continue;
                        }
                    };
                    let offered: Vec<String> = offers
                        .iter()
                        .map(|offer| format!("{}[{}]", offer.action.verb(), offer.reason))
                        .collect();
                    let _ = writeln!(out, "CONDITION|OFFERS|{label}|{}", offered.join(","));
                    if offers.iter().all(|offer| offer.action.verb() == "cancel") {
                        let _ =
                            writeln!(out, "CONDITION|DEAD_INITIAL|{label}|{}", offered.join(","));
                    }
                    for action in &all_verbs {
                        if offers
                            .iter()
                            .any(|offer| offer.action.verb() == action.verb())
                        {
                            continue;
                        }
                        match service
                            .perform_task_action(task.id.clone(), action.clone(), task.version)
                            .await
                        {
                            Err(ServiceError::TaskActionUnavailable { .. }) => {}
                            Ok(_) => {
                                let _ = writeln!(
                                    out,
                                    "CONDITION|ABSENT_ACCEPTED|{label}|{}",
                                    action.verb()
                                );
                            }
                            Err(error) => {
                                let _ = writeln!(
                                    out,
                                    "CONDITION|ABSENT_OTHER_ERR|{label}|{}|{}",
                                    action.verb(),
                                    condition_short(&error)
                                );
                            }
                        }
                    }
                    let mut attempts: Vec<(String, TaskAction)> = Vec::new();
                    for offer in &offers {
                        if offer.action.verb() == "cancel" {
                            continue;
                        }
                        attempts.push((
                            format!("{}[{}]", offer.action.verb(), offer.reason),
                            offer.action.clone(),
                        ));
                        if let TaskAction::Retry {
                            reason: None,
                            fresh_session,
                            refresh_workspace,
                            reset_budget,
                            guidance,
                        } = &offer.action
                        {
                            for parameter in &offer.parameters {
                                match parameter.name.as_str() {
                                    "fresh_session" => {
                                        let flipped = !fresh_session.unwrap_or(false);
                                        attempts.push((
                                            format!(
                                                "retry[{}]+fresh_session={flipped}",
                                                offer.reason
                                            ),
                                            TaskAction::Retry {
                                                reason: None,
                                                fresh_session: Some(flipped),
                                                refresh_workspace: *refresh_workspace,
                                                reset_budget: *reset_budget,
                                                guidance: guidance.clone(),
                                            },
                                        ));
                                    }
                                    "refresh_workspace" => {
                                        attempts.push((
                                            format!(
                                                "retry[{}]+refresh_workspace=true",
                                                offer.reason
                                            ),
                                            TaskAction::Retry {
                                                reason: None,
                                                fresh_session: *fresh_session,
                                                refresh_workspace: Some(true),
                                                reset_budget: *reset_budget,
                                                guidance: guidance.clone(),
                                            },
                                        ));
                                    }
                                    "reset_budget" => {
                                        for value in
                                            parameter.boolean_values.clone().unwrap_or_default()
                                        {
                                            if Some(value) != *reset_budget {
                                                attempts.push((
                                                    format!(
                                                        "retry[{}]+reset_budget={value}",
                                                        offer.reason
                                                    ),
                                                    TaskAction::Retry {
                                                        reason: None,
                                                        fresh_session: *fresh_session,
                                                        refresh_workspace: *refresh_workspace,
                                                        reset_budget: Some(value),
                                                        guidance: guidance.clone(),
                                                    },
                                                ));
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        if let TaskAction::Approve {
                            override_checks, ..
                        } = &offer.action
                        {
                            for value in offer
                                .parameters
                                .iter()
                                .find(|parameter| parameter.name == "override")
                                .and_then(|parameter| parameter.boolean_values.clone())
                                .unwrap_or_default()
                            {
                                if Some(value) != *override_checks {
                                    attempts.push((
                                        format!("approve[{}]+override={value}", offer.reason),
                                        TaskAction::Approve {
                                            override_checks: Some(value),
                                            reason: None,
                                        },
                                    ));
                                }
                            }
                        }
                    }
                    for (tag, attempt) in &attempts {
                        let mut attempt = attempt.clone();
                        match &mut attempt {
                            TaskAction::Approve { reason, .. }
                            | TaskAction::Retry { reason, .. }
                            | TaskAction::Cancel { reason } => {
                                *reason = Some("Matrix operator reason".to_owned())
                            }
                            TaskAction::SendBack { guidance } => {
                                *guidance = "Matrix requested correction".to_owned()
                            }
                            _ => {}
                        }
                        eprintln!("MATRIX_PROGRESS {label} {tag}: command");
                        let copy = condition_fixture(
                            &db, &project, &agent, workflow, state, kind, variant,
                        )
                        .await;
                        let command = tokio::time::timeout(
                            Duration::from_secs(60),
                            service.perform_task_action(
                                copy.id.clone(),
                                attempt.clone(),
                                copy.version,
                            ),
                        )
                        .await;
                        let command_result = match command {
                            Err(_) => "CMD_TIMEOUT".to_owned(),
                            Ok(Ok(_)) => "ok".to_owned(),
                            Ok(Err(ServiceError::Db(
                                DbError::VersionConflict | DbError::TaskVersionConflict { .. },
                            ))) => "version_conflict".to_owned(),
                            Ok(Err(error)) => format!("CMD_ERR:{}", condition_short(&error)),
                        };
                        let mut dispatch_results = Vec::new();
                        for _ in 0..3 {
                            service.drain(&copy.id).await.unwrap();
                            let current = TaskRepo::get_by_id(&*db, &copy.id, false)
                                .await
                                .unwrap()
                                .unwrap();
                            if !condition_marker_present(&current) {
                                break;
                            }
                            eprintln!("MATRIX_PROGRESS {label} {tag}: dispatch");
                            let result = tokio::time::timeout(
                                Duration::from_secs(60),
                                service.dispatch_queued_recovery(&current),
                            )
                            .await;
                            dispatch_results.push(match result {
                                Err(_) => "DISPATCH_TIMEOUT".to_owned(),
                                Ok(Ok(true)) => "applied".to_owned(),
                                Ok(Ok(false)) => "not_applied".to_owned(),
                                Ok(Err(error)) => {
                                    format!("DISPATCH_ERR:{}", condition_short(&error))
                                }
                            });
                        }
                        let current = TaskRepo::get_by_id(&*db, &copy.id, false)
                            .await
                            .unwrap()
                            .unwrap();
                        let running = ExecutionRepo::list_running_by_task(&*db, &copy.id)
                            .await
                            .unwrap();
                        let marker = condition_marker_present(&current);
                        // The applied action's entry hook step is queued work
                        // in flight, like a running execution; offers wait
                        // (Cancel only) until it settles.
                        let hooks_pending = db::TaskStepRepo::entry_hooks_pending(&*db, &copy.id)
                            .await
                            .unwrap();
                        let after = service
                            .task_action_offers(&copy.id, &Actor::user(UserActionSource::Test))
                            .await
                            .map(|response| {
                                response
                                    .available_actions
                                    .iter()
                                    .map(|offer| {
                                        format!("{}[{}]", offer.action.verb(), offer.reason)
                                    })
                                    .collect::<Vec<_>>()
                                    .join(",")
                            })
                            .unwrap_or_else(|error| {
                                format!("OFFERS_ERR:{}", condition_short(&error))
                            });
                        let annotation_kind = current
                            .error_annotation
                            .as_deref()
                            .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
                            .and_then(|value| {
                                value.get("type").and_then(Value::as_str).map(str::to_owned)
                            })
                            .unwrap_or_else(|| "-".to_owned());
                        let terminal = matches!(current.status.as_str(), "done" | "cancelled");
                        let only_cancel = after
                            .split(',')
                            .all(|verb| verb.is_empty() || verb.starts_with("cancel["));
                        let mut flags = Vec::new();
                        if command_result == "ok" {
                            accepted += 1;
                        }
                        if command_result.starts_with("CMD_") {
                            command_errors += 1;
                            flags.push("PROPERTY_VIOLATION");
                        }
                        if dispatch_results
                            .iter()
                            .any(|result| result.starts_with("DISPATCH_"))
                        {
                            flags.push("DISPATCH_FAILED");
                        }
                        if marker && running.is_empty() {
                            flags.push("MARKER_STUCK");
                        }
                        if !terminal && running.is_empty() && !hooks_pending && only_cancel {
                            flags.push("DEAD_END");
                            if command_result == "ok" {
                                dead_ends += 1;
                            }
                        }
                        if command_result == "ok" && !marker {
                            if dispatch_results
                                .iter()
                                .any(|result| result.starts_with("DISPATCH_ERR:"))
                            {
                                assert!(
                                    current.error_annotation.is_some()
                                        || current.blocked_json.is_some()
                                        || current.failed_json.is_some(),
                                    "{label} {tag}: refusal lost its condition; dispatch={dispatch_results:?}; current={current:?}"
                                );
                                restored += 1;
                            } else {
                                applied += 1;
                            }
                        }
                        let _ = writeln!(
                            out,
                            "CONDITION|APPLY|{label}|{tag}|cmd={command_result}|dispatch={}|status={}|ann={annotation_kind}|marker={marker}|running={}|after={after}|{}",
                            dispatch_results.join(">"),
                            current.status,
                            running.len(),
                            flags.join("+"),
                        );
                        sqlx::query(
                            "UPDATE execution SET status = 'cancelled' WHERE status = 'running'",
                        )
                        .execute(db.pool())
                        .await
                        .unwrap();
                    }
                }
            }
        }
    }
    let _ = writeln!(out, "MATRIX_RESULT snapshots={snapshots} accepted={accepted} applied={applied} restored_with_error={restored} dead_ends={dead_ends} command_errors={command_errors}");
    std::fs::create_dir_all(crate::task_actions::action_test_evidence_dir()).unwrap();
    std::fs::write(
        crate::task_actions::action_test_evidence_dir().join("db-matrix.txt"),
        out,
    )
    .unwrap();
    assert_eq!(
        command_errors, 0,
        "advertised parameter values must be accepted; see db-matrix.txt"
    );
    assert_eq!(
        dead_ends, 0,
        "accepted actions must not dead-end; see db-matrix.txt"
    );
    assert_eq!(
        accepted,
        applied + restored,
        "each accepted action must settle; see db-matrix.txt"
    );
}

#[tokio::test]
async fn queued_role_retry_preserves_a_condition_when_dispatch_refuses() {
    let db = Arc::new(sqlite_db().await);
    let (project, _, repo) = seed_project_repo(&db).await;
    initialize_primary_repository(&repo);
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
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(repo.path().join("workspaces"))
        .with_task_executor(Arc::new(ConditionPendingExecutor));
    let task = condition_fixture(
        &db,
        &project,
        &agent,
        "std",
        "planning",
        Some(FailureKind::ExecutorUnavailable),
        "bare",
    )
    .await;
    let action = service
        .task_action_offers(&task.id, &Actor::user(UserActionSource::Test))
        .await
        .unwrap()
        .available_actions
        .into_iter()
        .find(|offer| offer.action.verb() == "retry" && offer.reason == "role_retry")
        .unwrap()
        .action;
    service
        .perform_task_action(&task.id, action, task.version)
        .await
        .unwrap();
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let outcome = service.dispatch_queued_recovery(&task).await;
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    if outcome.is_err() {
        assert!(
            condition_marker_present(&current)
                || current.error_annotation.is_some()
                || current.blocked_json.is_some()
                || current.failed_json.is_some(),
            "refusal lost condition: {outcome:?}, original={:?}, current={current:?}",
            task.metadata_json
        );
    }
}
