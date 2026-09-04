use std::sync::Arc;

use chrono::{Duration, Utc};
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentRepo, AgentStatus,
    AttentionRepo, CreateAgentIdentity, CreateAgentProfile, CreateAttentionProjection,
    CreateDomainEvent, CreateExecution, CreateProject, CreateTask, DomainEventRepo, ExecutionRepo,
    ExecutionStatus, ProjectRepo, ResumePolicy, SqliteDb, TaskRepo, UpdateAttentionLifecycle,
};
use services::{
    workflow::{default_autonomous_workflow, default_workflow},
    AttentionService, WakeAdmissionRequest, WakeAdmissionResult, WakeSuppressionReason,
};
use tokio::sync::watch;

async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    Arc::new(SqliteDb::new(pool))
}

async fn identity(db: &SqliteDb, id: &str) {
    let now = now_rfc3339();
    AgentRepo::create_identity_with_profile(
        db,
        CreateAgentIdentity {
            id: id.to_owned(),
            name: "wake-test".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: new_uuid_v4(),
            identity_id: id.to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
}

/// Create a Project with the supplied identity as its active Project Agent.
/// Project creation seeds the setup-required binding; this helper only fills
/// in the binding fields that wake admission requires.
async fn configured_project(db: &Arc<SqliteDb>, identity_id: &str, name: &str) -> String {
    let profile_id: String =
        sqlx::query_scalar("SELECT selected_profile_id FROM agent_identity WHERE id = ?")
            .bind(identity_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    let project_id = new_uuid_v4();
    let now = now_rfc3339();
    ProjectRepo::create(
        &**db,
        CreateProject {
            id: project_id.clone(),
            name: name.to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    sqlx::query(
        "UPDATE project_agent_binding
         SET identity_id = ?, profile_id = ?, state = 'active', wake_budget = 10,
             version = version + 1, updated_at = ?
         WHERE project_id = ? AND state = 'agent_setup_required'",
    )
    .bind(identity_id)
    .bind(profile_id)
    .bind(&now)
    .bind(&project_id)
    .execute(db.pool())
    .await
    .unwrap();
    project_id
}

async fn project_task(db: &Arc<SqliteDb>, project_id: &str, title: &str) -> db::Task {
    let now = now_rfc3339();
    TaskRepo::create(
        &**db,
        CreateTask {
            id: new_uuid_v4(),
            project_id: project_id.to_owned(),
            repo_id: None,
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: title.to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "in_progress".to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap()
}

async fn task_execution(
    db: &SqliteDb,
    task: &db::Task,
    identity_id: &str,
    status: ExecutionStatus,
    created_at: &str,
    parent_execution_id: Option<String>,
) -> db::Execution {
    let failed = status == ExecutionStatus::Failed;
    ExecutionRepo::create(
        db,
        CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(identity_id.to_owned()),
            role: "coder".to_owned(),
            status,
            stop_reason: failed.then_some(db::StopReason::ExecutorFailed),
            stopped_by: failed.then(|| "system:executor".to_owned()),
            resume_policy: failed.then_some(ResumePolicy::Manual),
            stopped_at: failed.then(|| created_at.to_owned()),
            parent_execution_id,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: created_at.to_owned(),
            updated_at: created_at.to_owned(),
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn user_pause_and_stop_keep_manual_controls_without_recovery_wakes() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "intentional-stops").await;
    let tasks = services::TaskService::new(Arc::clone(&db), Arc::new(events::EventBus::default()));
    let attention = AttentionService::new(Arc::clone(&db));

    for pause in [true, false] {
        let task = project_task(&db, &project_id, "Wait for the user to resume").await;
        let execution = task_execution(
            &db,
            &task,
            &identity_id,
            ExecutionStatus::Running,
            &now_rfc3339(),
            None,
        )
        .await;
        if pause {
            tasks
                .pause_execution(&execution.id, "Inspect this work".to_owned())
                .await
                .unwrap();
        } else {
            tasks
                .cancel_execution(&execution.id, "Stop this work".to_owned())
                .await
                .unwrap();
        }
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let annotation: serde_json::Value =
            serde_json::from_str(current.error_annotation.as_deref().unwrap()).unwrap();
        assert_eq!(annotation["type"], "manual_stop");
        assert!(!annotation["recovery_actions"]
            .as_array()
            .unwrap()
            .is_empty());
        let run = attention.project_once(100).await.unwrap();
        assert_eq!(
            run.processed_events, run.claimed_events,
            "intentional stops must not hold the event cursor for orphan grace"
        );
        let payload: String = sqlx::query_scalar("SELECT payload_json FROM domain_event WHERE event_type = 'task.interruption_changed' AND entity_id = ? ORDER BY sequence DESC LIMIT 1")
            .bind(&task.id).fetch_one(db.pool()).await.unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&payload).unwrap()["requires_intervention"],
            false
        );
    }
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM attention_projection WHERE scope_id = ? AND attention_type = 'execution_failed'")
        .bind(&project_id).fetch_one(db.pool()).await.unwrap(), 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_wake_lease WHERE scope_id = ?")
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn superseded_terminal_attempts_never_become_orphans_or_delay_other_events() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "recovered-attempts").await;
    let attention = AttentionService::new(Arc::clone(&db));
    for age_seconds in [0, 60] {
        for (successor_status, linked) in [
            (ExecutionStatus::Running, true),
            (ExecutionStatus::Completed, true),
            (ExecutionStatus::Running, false),
            (ExecutionStatus::Completed, false),
        ] {
            let task = project_task(&db, &project_id, "Already recovered").await;
            let terminal_at = (Utc::now() - Duration::seconds(age_seconds)).to_rfc3339();
            let stopped = task_execution(
                &db,
                &task,
                &identity_id,
                ExecutionStatus::Failed,
                &terminal_at,
                None,
            )
            .await;
            append_attention_event_at(&db, "execution.failed", &task.id, &project_id,
                serde_json::json!({"execution_id": stopped.id, "task_id": task.id, "role": "coder", "status": "failed"}), &terminal_at).await;
            // The successor is authoritative even when its creation timestamp
            // is identical to its parent (for example a coarse imported clock).
            // A newer independent re-execution need not link to the old one.
            let successor_at = if linked {
                terminal_at.clone()
            } else {
                (Utc::now() + Duration::seconds(1)).to_rfc3339()
            };
            task_execution(
                &db,
                &task,
                &identity_id,
                successor_status,
                &successor_at,
                linked.then_some(stopped.id),
            )
            .await;
        }
    }
    let run = attention.project_once(100).await.unwrap();
    assert_eq!(
        run.processed_events, run.claimed_events,
        "old failures must not delay unrelated events"
    );
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM attention_projection WHERE scope_id = ? AND attention_type = 'execution_failed'")
        .bind(&project_id).fetch_one(db.pool()).await.unwrap(), 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_wake_lease WHERE scope_id = ?")
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn obsolete_orphan_wakes_recheck_task_and_attempt_before_spending_budget() {
    for outcome in [
        "running",
        "completed",
        "cancelled",
        "deleted",
        "deferred",
        "manual_stop",
    ] {
        let db = database().await;
        let identity_id = new_uuid_v4();
        identity(&db, &identity_id).await;
        let project_id = configured_project(&db, &identity_id, outcome).await;
        // Project the incident while the responder is unavailable, then change
        // execution/Task truth before retrying admission for the open incident.
        sqlx::query("UPDATE project_agent_binding SET state = 'paused' WHERE project_id = ?")
            .bind(&project_id)
            .execute(db.pool())
            .await
            .unwrap();
        let task = project_task(&db, &project_id, "Recovery already settled").await;
        let terminal_at = (Utc::now() - Duration::minutes(1)).to_rfc3339();
        let stopped = task_execution(
            &db,
            &task,
            &identity_id,
            ExecutionStatus::Failed,
            &terminal_at,
            None,
        )
        .await;
        append_attention_event_at(
            &db,
            "execution.failed",
            &task.id,
            &project_id,
            serde_json::json!({"execution_id": stopped.id, "task_id": task.id}),
            &terminal_at,
        )
        .await;
        let attention = AttentionService::new(Arc::clone(&db));
        attention.project_once(100).await.unwrap();
        let (id, incident_key, source_event_id, status): (String, String, String, String) =
            sqlx::query_as(
                "SELECT id, dedupe_key, source_event_id, status FROM attention_projection
             WHERE scope_id = ? AND attention_type = 'execution_failed'",
            )
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(status, "open");
        match outcome {
            "running" | "completed" => {
                let status = if outcome == "running" {
                    ExecutionStatus::Running
                } else {
                    ExecutionStatus::Completed
                };
                task_execution(
                    &db,
                    &task,
                    &identity_id,
                    status,
                    &terminal_at,
                    Some(stopped.id),
                )
                .await;
            }
            "cancelled" => {
                TaskRepo::update_status(
                    &*db,
                    db::UpdateTaskStatus {
                        id: task.id.clone(),
                        expected_version: task.version,
                        status: "cancelled".to_owned(),
                        assignee_id: None,
                        error_annotation: None,
                        blocked_json: None,
                        failed_json: None,
                        updated_at: now_rfc3339(),
                    },
                )
                .await
                .unwrap();
            }
            "deleted" => {
                sqlx::query("UPDATE task SET deleted_at = ?, version = version + 1 WHERE id = ?")
                    .bind(now_rfc3339())
                    .bind(&task.id)
                    .execute(db.pool())
                    .await
                    .unwrap();
            }
            "deferred" => {
                TaskRepo::set_metadata_json(
                    &*db,
                    &task.id,
                    Some(
                        serde_json::json!({
                            "deferred_dispatch": {
                                "not_before": (Utc::now() + Duration::minutes(1)).to_rfc3339(),
                                "reason": "retry scheduled", "target_state": "in_progress"
                            }
                        })
                        .to_string(),
                    ),
                    &now_rfc3339(),
                )
                .await
                .unwrap();
            }
            "manual_stop" => {
                sqlx::query(
                    "UPDATE task SET error_annotation = ?, version = version + 1 WHERE id = ?",
                )
                .bind(
                    serde_json::json!({"type": "manual_stop", "recovery_actions": ["reexecute"]})
                        .to_string(),
                )
                .bind(&task.id)
                .execute(db.pool())
                .await
                .unwrap();
            }
            _ => unreachable!(),
        }
        sqlx::query("UPDATE project_agent_binding SET state = 'active' WHERE project_id = ?")
            .bind(&project_id)
            .execute(db.pool())
            .await
            .unwrap();
        let result = attention
            .admit_wake(WakeAdmissionRequest {
                scope_type: "project".to_owned(),
                scope_id: project_id.clone(),
                causation_id: Some(source_event_id),
                now: now_rfc3339(),
                ..request(&identity_id, &incident_key)
            })
            .await
            .unwrap();
        assert!(
            matches!(
                result,
                WakeAdmissionResult::Suppressed {
                    reason: WakeSuppressionReason::ResolvedIncident
                }
            ),
            "{outcome}: {result:?}"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM attention_projection WHERE id = ?")
                .bind(&id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            "resolved",
            "{outcome}"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM agent_wake_budget_window WHERE scope_id = ?"
            )
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            0,
            "{outcome}"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM agent_wake_lease WHERE scope_id = ?"
            )
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            0,
            "{outcome}"
        );
    }
}

async fn append_attention_event(
    db: &Arc<SqliteDb>,
    event_type: &str,
    task_id: &str,
    project_id: &str,
    payload: serde_json::Value,
) {
    append_attention_event_at(db, event_type, task_id, project_id, payload, &now_rfc3339()).await;
}

async fn append_attention_event_at(
    db: &Arc<SqliteDb>,
    event_type: &str,
    task_id: &str,
    project_id: &str,
    payload: serde_json::Value,
    created_at: &str,
) {
    DomainEventRepo::append_event(
        &**db,
        CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: event_type.to_owned(),
            entity_type: "task".to_owned(),
            entity_id: task_id.to_owned(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project_id.to_owned(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(new_uuid_v4()),
            payload_json: payload.to_string(),
            created_at: created_at.to_owned(),
        },
    )
    .await
    .unwrap();
}

fn request(identity_id: &str, incident_key: &str) -> WakeAdmissionRequest {
    WakeAdmissionRequest {
        identity_id: identity_id.to_owned(),
        scope_type: "account".to_owned(),
        scope_id: "account-1".to_owned(),
        incident_key: incident_key.to_owned(),
        lease_owner: "worker-1".to_owned(),
        correlation_id: "correlation-1".to_owned(),
        causation_id: Some("event-1".to_owned()),
        caused_by_identity_id: None,
        reaction_depth: 0,
        now: "2026-01-01T00:00:00Z".to_owned(),
        lease_seconds: 30,
        cooldown_seconds: 60,
    }
}

#[tokio::test]
async fn wake_admission_deduplicates_and_suppresses_recursive_events() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let service = AttentionService::new(Arc::clone(&db));

    let first = service
        .admit_wake(request(&identity_id, "incident-1"))
        .await
        .unwrap();
    assert!(matches!(first, WakeAdmissionResult::Admitted { .. }));

    let duplicate = service
        .admit_wake(request(&identity_id, "incident-1"))
        .await
        .unwrap();
    assert!(matches!(
        duplicate,
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::DuplicateIncident
        }
    ));

    let mut recursive = request(&identity_id, "incident-2");
    recursive.reaction_depth = 9;
    assert!(matches!(
        service.admit_wake(recursive).await.unwrap(),
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::ReactionDepthExceeded
        }
    ));

    let mut self_event = request(&identity_id, "incident-3");
    self_event.reaction_depth = 1;
    self_event.caused_by_identity_id = Some(identity_id.clone());
    assert!(matches!(
        service.admit_wake(self_event).await.unwrap(),
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::SelfEvent
        }
    ));

    let mut depth_boundary = request(&identity_id, "incident-depth-boundary");
    depth_boundary.reaction_depth = 8;
    assert!(matches!(
        service.admit_wake(depth_boundary).await.unwrap(),
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::ReactionDepthExceeded
        }
    ));

    let mut retry_exhausted = request(&identity_id, "attention:retry_exhausted:agent_chat:chat-1");
    retry_exhausted.scope_type = "agent_chat".to_owned();
    retry_exhausted.scope_id = "chat-1".to_owned();
    assert!(matches!(
        service.admit_wake(retry_exhausted).await.unwrap(),
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::RecursiveAgentResponse
        }
    ));

    let suppressed_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'agent.wake.suppressed'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert!(suppressed_count >= 3);
}

#[tokio::test]
async fn wake_policy_persists_cooldown_budget_and_global_identity_suppression() {
    let db = database().await;
    let first_identity = new_uuid_v4();
    let replacement_identity = new_uuid_v4();
    identity(&db, &first_identity).await;
    identity(&db, &replacement_identity).await;
    let service = AttentionService::new(Arc::clone(&db));

    let mut first = request(&first_identity, "incident-cooldown");
    first.causation_id = Some("source-cooldown-1".to_owned());
    assert!(matches!(
        service.admit_wake(first.clone()).await.unwrap(),
        WakeAdmissionResult::Admitted { .. }
    ));

    let mut cooldown = first.clone();
    cooldown.lease_owner = "replacement-worker".to_owned();
    cooldown.causation_id = Some("source-cooldown-2".to_owned());
    cooldown.now = "2026-01-01T00:00:31Z".to_owned();
    assert!(matches!(
        service.admit_wake(cooldown).await.unwrap(),
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::Cooldown
        }
    ));

    // Once the cooldown expires, replaying the same source decision returns
    // its original admission metadata without charging the wake budget a
    // second time.
    let mut replay = first.clone();
    replay.now = "2026-01-01T00:10:00Z".to_owned();
    assert!(matches!(
        service.admit_wake(replay).await.unwrap(),
        WakeAdmissionResult::Admitted { .. }
    ));
    let replay_count: i64 = sqlx::query_scalar(
        "SELECT admitted_count FROM agent_wake_budget_window
         WHERE identity_id = ? AND scope_type = 'account' AND scope_id = 'account-1'",
    )
    .bind(&first_identity)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(replay_count, 1);

    let mut replacement = request(&replacement_identity, "incident-global");
    replacement.causation_id = Some("source-global-1".to_owned());
    assert!(matches!(
        service.admit_wake(replacement.clone()).await.unwrap(),
        WakeAdmissionResult::Admitted { .. }
    ));
    replacement.now = "2026-01-01T00:00:10Z".to_owned();
    replacement.identity_id = first_identity.clone();
    replacement.lease_owner = "old-binding-worker".to_owned();
    replacement.causation_id = Some("source-global-2".to_owned());
    assert!(matches!(
        service.admit_wake(replacement).await.unwrap(),
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::DuplicateIncident
        }
    ));

    // Saturating the persisted window must not be counted again by a
    // suppression decision.
    sqlx::query(
        "INSERT INTO agent_wake_budget_window (
             identity_id, scope_type, scope_id, window_started_at,
             window_seconds, admitted_count, version, updated_at
         ) VALUES (?, 'account', 'account-1', ?, 3600, 10, 1, ?)
         ON CONFLICT(identity_id, scope_type, scope_id) DO UPDATE SET
             window_started_at = excluded.window_started_at,
             admitted_count = excluded.admitted_count,
             version = agent_wake_budget_window.version + 1,
             updated_at = excluded.updated_at",
    )
    .bind(&first_identity)
    .bind("2026-01-01T00:00:00Z")
    .bind("2026-01-01T00:00:00Z")
    .execute(db.pool())
    .await
    .unwrap();
    let mut budget = request(&first_identity, "incident-budget");
    budget.now = "2026-01-01T00:00:30Z".to_owned();
    budget.causation_id = Some("source-budget".to_owned());
    assert!(matches!(
        service.admit_wake(budget).await.unwrap(),
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::BudgetExhausted
        }
    ));
    let admitted_count: i64 = sqlx::query_scalar(
        "SELECT admitted_count FROM agent_wake_budget_window
         WHERE identity_id = ? AND scope_type = 'account' AND scope_id = 'account-1'",
    )
    .bind(&first_identity)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(admitted_count, 10);

    let decisions: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event
         WHERE event_type = 'agent.wake.suppressed'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert!(decisions >= 2);
}

#[tokio::test]
async fn identical_suppressed_wake_decision_is_deduped() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let service = AttentionService::new(Arc::clone(&db));
    let mut request = request(&identity_id, "incident-deduped-suppression");
    request.reaction_depth = 8;

    for _ in 0..2 {
        assert!(matches!(
            service.admit_wake(request.clone()).await.unwrap(),
            WakeAdmissionResult::Suppressed {
                reason: WakeSuppressionReason::ReactionDepthExceeded
            }
        ));
    }
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event
         WHERE event_type = 'agent.wake.suppressed'
           AND json_extract(payload_json, '$.reason') = 'reaction_depth_exceeded'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn retry_exhausted_agent_chat_is_suppressed_before_binding_setup() {
    let db = database().await;
    let project_id = new_uuid_v4();
    let turn_job_id = new_uuid_v4();
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO project (
             id, name, settings, workflow_definition, owner_id, created_at, updated_at
         ) VALUES (?, 'retry-suppression-project', '{}', '{}', NULL, ?, ?)",
    )
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .unwrap();
    let chat_id: String =
        sqlx::query_scalar("SELECT id FROM agent_chat WHERE project_id = ? AND kind = 'project'")
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    sqlx::query("UPDATE agent_chat SET status = 'ready' WHERE id = ?")
        .bind(&chat_id)
        .execute(db.pool())
        .await
        .unwrap();
    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "agent_chat.turn.failed".to_owned(),
            entity_type: "agent_chat_turn_job".to_owned(),
            entity_id: turn_job_id,
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "agent_chat".to_owned(),
            scope_id: chat_id.clone(),
            correlation_id: "retry-suppression-correlation".to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("retry-suppression-source".to_owned()),
            payload_json: r#"{"status":"failed"}"#.to_owned(),
            created_at: now,
        },
    )
    .await
    .unwrap();

    let service = AttentionService::new(Arc::clone(&db));
    service.project_once(100).await.unwrap();
    let (reason, attention_scope): (String, String) = sqlx::query_as(
        "SELECT
            json_extract(payload_json, '$.reason'),
            (SELECT scope_type FROM attention_projection LIMIT 1)
         FROM domain_event
         WHERE event_type = 'agent.wake.suppressed'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(reason, "retry_exhausted_same_chat");
    assert_eq!(attention_scope, "project");
    let budget_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_wake_budget_window")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(budget_count, 0);
}

#[tokio::test]
async fn binding_replacement_cannot_create_a_second_active_incident_lease() {
    let db = database().await;
    let old_identity = new_uuid_v4();
    let new_identity = new_uuid_v4();
    identity(&db, &old_identity).await;
    identity(&db, &new_identity).await;
    let old_profile: String =
        sqlx::query_scalar("SELECT selected_profile_id FROM agent_identity WHERE id = ?")
            .bind(&old_identity)
            .fetch_one(db.pool())
            .await
            .unwrap();
    let new_profile: String =
        sqlx::query_scalar("SELECT selected_profile_id FROM agent_identity WHERE id = ?")
            .bind(&new_identity)
            .fetch_one(db.pool())
            .await
            .unwrap();
    let project_id = new_uuid_v4();
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO project (
             id, name, settings, workflow_definition, owner_id, created_at, updated_at
         ) VALUES (?, 'binding-replacement-project', '{}', '{}', NULL, ?, ?)",
    )
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .unwrap();
    let binding_id: String = sqlx::query_scalar(
        "SELECT id FROM project_agent_binding
         WHERE project_id = ? AND state = 'agent_setup_required'",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE project_agent_binding
         SET identity_id = ?, profile_id = ?, state = 'active', wake_budget = 10,
             version = version + 1, updated_at = ?
         WHERE id = ?",
    )
    .bind(&old_identity)
    .bind(&old_profile)
    .bind(&now)
    .bind(&binding_id)
    .execute(db.pool())
    .await
    .unwrap();
    let entity_id = new_uuid_v4();
    let first_source = new_uuid_v4();
    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: first_source,
            event_type: "runtime.connection_unavailable".to_owned(),
            entity_type: "agent_session".to_owned(),
            entity_id: entity_id.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: "replacement-correlation-1".to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("replacement-source-1".to_owned()),
            payload_json: r#"{"status":"unavailable"}"#.to_owned(),
            created_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let service = AttentionService::new(Arc::clone(&db));
    service.project_once(100).await.unwrap();
    let admitted_identity: String = sqlx::query_scalar(
        "SELECT json_extract(payload_json, '$.identity_id') FROM domain_event
         WHERE event_type = 'agent.wake.admitted'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(admitted_identity, old_identity);

    sqlx::query(
        "UPDATE project_agent_binding
         SET state = 'replaced', version = version + 1, updated_at = ?
         WHERE id = ? AND state = 'active'",
    )
    .bind(&now)
    .bind(&binding_id)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO project_agent_binding (
             id, project_id, identity_id, profile_id, state,
             autonomy_policy_json, permission_ceiling_json, subscriptions_json,
             wake_budget, version, created_at, updated_at
         ) VALUES (?, ?, ?, ?, 'active', '{}', '{}', '[]', 10, 1, ?, ?)",
    )
    .bind(new_uuid_v4())
    .bind(&project_id)
    .bind(&new_identity)
    .bind(&new_profile)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .unwrap();
    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "runtime.connection_unavailable".to_owned(),
            entity_type: "agent_session".to_owned(),
            entity_id: entity_id.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: "replacement-correlation-2".to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("replacement-source-2".to_owned()),
            payload_json: r#"{"status":"unavailable"}"#.to_owned(),
            created_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    service.project_once(100).await.unwrap();
    let duplicate_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event
         WHERE event_type = 'agent.wake.suppressed'
           AND json_extract(payload_json, '$.reason') = 'duplicate_incident'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(duplicate_count, 1);
    let lease_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_wake_lease
         WHERE incident_key = 'attention:runtime_offline:project:' || ? || ':agent_session:' || ?",
    )
    .bind(&project_id)
    .bind(&entity_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(lease_count, 1);
}

#[tokio::test]
async fn resolved_attention_is_suppressed_with_incident_reference() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let source_event_id = new_uuid_v4();
    let entity_id = new_uuid_v4();
    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: source_event_id.clone(),
            event_type: "runtime.connection_unavailable".to_owned(),
            entity_type: "agent_session".to_owned(),
            entity_id: entity_id.clone(),
            actor_type: "agent".to_owned(),
            actor_id: Some(identity_id.clone()),
            scope_type: "account".to_owned(),
            scope_id: "account-1".to_owned(),
            correlation_id: "resolved-correlation".to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("resolved-source".to_owned()),
            payload_json: r#"{"status":"unavailable"}"#.to_owned(),
            created_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let incident_key =
        format!("attention:runtime_offline:account:account-1:agent_session:{entity_id}");
    let attention = AttentionRepo::insert_attention(
        &*db,
        CreateAttentionProjection {
            id: new_uuid_v4(),
            attention_type: "runtime_offline".to_owned(),
            scope_type: "account".to_owned(),
            scope_id: "account-1".to_owned(),
            identity_id: Some(identity_id.clone()),
            source_event_id: source_event_id.clone(),
            priority: 95,
            status: "open".to_owned(),
            summary: "Agent runtime is unavailable".to_owned(),
            details_json: "{}".to_owned(),
            dedupe_key: incident_key.clone(),
            occurred_at: now_rfc3339(),
            updated_at: now_rfc3339(),
            acknowledged_at: None,
            snoozed_until: None,
            resolved_at: None,
            updated_by_user_id: None,
            recommended_action: "restore_runtime".to_owned(),
            source_sequence: Some(1),
        },
    )
    .await
    .unwrap();
    AttentionRepo::update_attention_lifecycle(
        &*db,
        UpdateAttentionLifecycle {
            id: attention.id.clone(),
            expected_version: attention.version,
            status: "resolved".to_owned(),
            acknowledged_at: None,
            snoozed_until: Some(None),
            resolved_at: Some(Some(now_rfc3339())),
            updated_by_user_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();

    let result = AttentionService::new(Arc::clone(&db))
        .admit_wake(WakeAdmissionRequest {
            identity_id,
            scope_type: "account".to_owned(),
            scope_id: "account-1".to_owned(),
            incident_key,
            lease_owner: "resolved-worker".to_owned(),
            correlation_id: "resolved-correlation".to_owned(),
            causation_id: Some(source_event_id),
            caused_by_identity_id: None,
            reaction_depth: 0,
            now: "2026-01-01T00:00:00Z".to_owned(),
            lease_seconds: 30,
            cooldown_seconds: 60,
        })
        .await
        .unwrap();
    assert!(matches!(
        result,
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::ResolvedIncident
        }
    ));
    let payload: serde_json::Value = sqlx::query_scalar(
        "SELECT payload_json FROM domain_event
         WHERE event_type = 'agent.wake.suppressed'
           AND json_extract(payload_json, '$.reason') = 'resolved_incident'",
    )
    .fetch_one(db.pool())
    .await
    .map(|value: String| serde_json::from_str(&value).unwrap())
    .unwrap();
    assert_eq!(payload["attention_id"], attention.id);
    assert_eq!(payload["source_event_id"], attention.source_event_id);
}

#[tokio::test]
async fn recovered_task_suppresses_a_stale_interruption_wake_before_budget() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "stale-recovery-project").await;
    let task = project_task(&db, &project_id, "Already recovered").await;
    let source_event = DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "task.interruption_changed".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: task.id.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "task".to_owned(),
            scope_id: task.id.clone(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(new_uuid_v4()),
            payload_json: serde_json::json!({
                "task_id": task.id,
                "task_version": task.version,
                "requires_intervention": true,
                "interruption": {
                    "source": "blocked",
                    "recovery_actions": ["reexecute"]
                }
            })
            .to_string(),
            created_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let incident_key = format!(
        "attention:execution_failed:project:{project_id}:task:{}",
        task.id
    );
    let attention = AttentionRepo::insert_attention(
        &*db,
        CreateAttentionProjection {
            id: new_uuid_v4(),
            attention_type: "execution_failed".to_owned(),
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            identity_id: Some(identity_id.clone()),
            source_event_id: source_event.id.clone(),
            priority: 85,
            status: "open".to_owned(),
            summary: "Task needs recovery".to_owned(),
            details_json: serde_json::json!({
                "source_event_type": "task.interruption_changed",
                "entity_type": "task",
                "entity_id": task.id,
                "scope_type": "project",
                "scope_id": project_id,
                "recovery": {
                    "requires_intervention": true,
                    "actions": ["reexecute"],
                    "automatic_retry": false
                }
            })
            .to_string(),
            dedupe_key: incident_key.clone(),
            occurred_at: source_event.created_at.clone(),
            updated_at: now_rfc3339(),
            acknowledged_at: None,
            snoozed_until: None,
            resolved_at: None,
            updated_by_user_id: None,
            recommended_action: "inspect_task".to_owned(),
            source_sequence: Some(source_event.sequence),
        },
    )
    .await
    .unwrap();

    let result = AttentionService::new(Arc::clone(&db))
        .admit_wake(WakeAdmissionRequest {
            identity_id,
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            incident_key,
            lease_owner: "stale-recovery-worker".to_owned(),
            correlation_id: source_event.correlation_id,
            causation_id: Some(source_event.id),
            caused_by_identity_id: None,
            reaction_depth: 0,
            now: now_rfc3339(),
            lease_seconds: 30,
            cooldown_seconds: 60,
        })
        .await
        .unwrap();
    assert!(matches!(
        result,
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::ResolvedIncident
        }
    ));
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM attention_projection WHERE id = ?")
            .bind(&attention.id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
        "resolved"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_wake_budget_window WHERE scope_id = ?",
        )
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'agent.wake.admitted' AND scope_id = ?",
        )
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn projection_worker_drains_events_reports_health_and_stops() {
    let db = database().await;
    let project_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO project (
            id, name, settings, workflow_definition, owner_id, created_at, updated_at
         ) VALUES (?, 'attention-worker-project', '{}', '{}', NULL, ?, ?)",
    )
    .bind(&project_id)
    .bind(now_rfc3339())
    .bind(now_rfc3339())
    .execute(db.pool())
    .await
    .unwrap();
    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "task.validation_failed".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: new_uuid_v4(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project_id,
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(new_uuid_v4()),
            payload_json: "{}".to_owned(),
            created_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();

    let service = std::sync::Arc::new(AttentionService::new(std::sync::Arc::clone(&db)));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = std::sync::Arc::clone(&service).start(shutdown_rx);
    let mut health = None;
    for _ in 0..100 {
        health = AttentionRepo::get_attention_consumer_health(&*db, "attention_projection")
            .await
            .unwrap();
        if health
            .as_ref()
            .is_some_and(|value| value.processed_events >= 1)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(health.is_some_and(|value| value.processed_events >= 1));

    shutdown_tx.send(true).unwrap();
    handle.await.unwrap();
}

#[tokio::test]
async fn semantic_progress_warning_is_typed_deduped_and_resolved_by_progress() {
    let db = database().await;
    let project_id = new_uuid_v4();
    let execution_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO project (
            id, name, settings, workflow_definition, owner_id, created_at, updated_at
         ) VALUES (?, 'progress-warning-project', '{}', '{}', NULL, ?, ?)",
    )
    .bind(&project_id)
    .bind(now_rfc3339())
    .bind(now_rfc3339())
    .execute(db.pool())
    .await
    .unwrap();

    let append = |db: Arc<SqliteDb>, id: String, event_type: &str, payload: String| {
        let event_type = event_type.to_owned();
        let project_id = project_id.clone();
        let execution_id = execution_id.clone();
        async move {
            DomainEventRepo::append_event(
                &*db,
                CreateDomainEvent {
                    id,
                    event_type,
                    entity_type: "execution".to_owned(),
                    entity_id: execution_id.clone(),
                    actor_type: "system".to_owned(),
                    actor_id: None,
                    scope_type: "project".to_owned(),
                    scope_id: project_id.clone(),
                    correlation_id: new_uuid_v4(),
                    causation_id: None,
                    causation_depth: 0,
                    dedupe_key: None,
                    payload_json: payload,
                    created_at: now_rfc3339(),
                },
            )
            .await
            .unwrap();
        }
    };

    append(
        Arc::clone(&db),
        new_uuid_v4(),
        "execution.progress_warning",
        serde_json::json!({
            "execution_id": execution_id,
            "episode_id": "episode-1",
            "owner_lease_live": true,
        })
        .to_string(),
    )
    .await;
    let service = AttentionService::new(Arc::clone(&db));
    service.project_once(100).await.unwrap();

    let first: (String, String, String, String) = sqlx::query_as(
        "SELECT attention_type, dedupe_key, status, source_event_id
         FROM attention_projection WHERE attention_type = 'progress_warning'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(first.0, "progress_warning");
    assert_eq!(
        first.1,
        format!("attention:progress_warning:project:{project_id}:execution:{execution_id}")
    );
    assert_eq!(first.2, "open");

    // A second warning for the same execution episode updates the one
    // projection row instead of creating a second Attention incident.
    let replay_event_id = new_uuid_v4();
    append(
        Arc::clone(&db),
        replay_event_id.clone(),
        "execution.progress_warning",
        serde_json::json!({
            "execution_id": execution_id,
            "episode_id": "episode-1",
            "owner_lease_live": true,
        })
        .to_string(),
    )
    .await;
    service.project_once(100).await.unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM attention_projection WHERE attention_type = 'progress_warning'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(count, 1);
    let source_event_id: String = sqlx::query_scalar(
        "SELECT source_event_id FROM attention_projection
         WHERE attention_type = 'progress_warning'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(source_event_id, replay_event_id);

    append(
        Arc::clone(&db),
        new_uuid_v4(),
        "execution.progressed",
        serde_json::json!({
            "execution_id": execution_id,
            "episode_id": "episode-1",
        })
        .to_string(),
    )
    .await;
    service.project_once(100).await.unwrap();
    let status: String = sqlx::query_scalar(
        "SELECT status FROM attention_projection
         WHERE attention_type = 'progress_warning'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(status, "resolved");
}

#[tokio::test]
async fn projected_incident_emits_one_durable_wake_action_and_replay_is_suppressed() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let source_event_id = new_uuid_v4();
    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: source_event_id.clone(),
            event_type: "runtime.connection_unavailable".to_owned(),
            entity_type: "agent_session".to_owned(),
            entity_id: new_uuid_v4(),
            actor_type: "agent".to_owned(),
            actor_id: Some(identity_id.clone()),
            scope_type: "account".to_owned(),
            scope_id: "account-1".to_owned(),
            correlation_id: "wake-correlation".to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("wake-source-event".to_owned()),
            payload_json: r#"{"status":"unavailable"}"#.to_owned(),
            created_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();

    let service = AttentionService::new(Arc::clone(&db));
    let run = service.project_once(100).await.unwrap();
    assert_eq!(run.processed_events, 1);

    // This fixture has an identity but no current Main binding.  Attention
    // remains durable while wake policy emits an explicit setup trigger; the
    // event is never silently consumed as if a turn had been delivered.
    let setup_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'agent.wake.setup_required'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(setup_count, 1);
    let payload: serde_json::Value = sqlx::query_scalar(
        "SELECT payload_json FROM domain_event
         WHERE event_type = 'agent.wake.setup_required'",
    )
    .fetch_one(db.pool())
    .await
    .map(|value: String| serde_json::from_str(&value).unwrap())
    .unwrap();
    assert_eq!(payload["decision"], "setup_required");
    assert_eq!(payload["reason"], "responder_binding_missing");
    assert_eq!(payload["source_event_id"], source_event_id);
    assert!(payload["attention_id"].as_str().is_some());
    assert!(payload["incident_digest"].as_str().is_some());
}

#[tokio::test]
async fn auto_retried_execution_failure_is_audit_only() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "auto-retry-project").await;
    let task = project_task(&db, &project_id, "Retry without waking the Project Agent").await;
    let execution_id = new_uuid_v4();
    let now = now_rfc3339();

    // Use a real terminal Execution row so the projection exercises the
    // retry/resume policy boundary rather than trusting prose in the event.
    ExecutionRepo::create(
        &*db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(identity_id),
            role: "coder".to_owned(),
            status: ExecutionStatus::Failed,
            stop_reason: Some(db::StopReason::ExecutorFailed),
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: Some(ResumePolicy::Auto),
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some("transient executor failure".to_owned()),
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    append_attention_event(
        &db,
        "execution.failed",
        &task.id,
        &project_id,
        serde_json::json!({
            "execution_id": execution_id,
            "task_id": task.id,
            "role": "coder",
            "status": "failed",
            "stop_reason": "executor_failed",
            "error": "transient executor failure",
        }),
    )
    .await;

    AttentionService::new(Arc::clone(&db))
        .project_once(100)
        .await
        .unwrap();

    let attention_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM attention_projection
         WHERE attention_type = 'execution_failed' AND scope_id = ?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    let wake_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event
         WHERE event_type = 'agent.wake.admitted' AND scope_id = ?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        attention_count, 0,
        "automatic retry must not create Attention"
    );
    assert_eq!(wake_count, 0, "automatic retry must not wake Project Agent");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_wake_lease WHERE scope_id = ?",)
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn undisposed_manual_execution_failure_becomes_actionable_after_grace() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "orphaned-failure-project").await;
    let task = project_task(&db, &project_id, "Recover an orphaned terminal attempt").await;
    let execution_id = new_uuid_v4();
    let terminal_at = (Utc::now() - Duration::minutes(1)).to_rfc3339();

    ExecutionRepo::create(
        &*db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(identity_id),
            role: "coder".to_owned(),
            status: ExecutionStatus::Failed,
            stop_reason: Some(db::StopReason::ExecutorFailed),
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: Some(ResumePolicy::Manual),
            stopped_at: Some(terminal_at.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some("terminalization committed before disposition".to_owned()),
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: terminal_at.clone(),
            updated_at: terminal_at.clone(),
        },
    )
    .await
    .unwrap();
    append_attention_event_at(
        &db,
        "execution.failed",
        &task.id,
        &project_id,
        serde_json::json!({
            "execution_id": execution_id,
            "task_id": task.id,
            "role": "coder",
            "status": "failed",
            "stop_reason": "executor_failed",
        }),
        &terminal_at,
    )
    .await;

    let service = AttentionService::new(Arc::clone(&db));
    service.project_once(100).await.unwrap();

    let attention: (String, String) = sqlx::query_as(
        "SELECT status, details_json FROM attention_projection
         WHERE attention_type = 'execution_failed' AND scope_id = ?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(attention.0, "open");
    let details: serde_json::Value = serde_json::from_str(&attention.1).unwrap();
    assert_eq!(details["source_event_type"], "execution.failed");
    assert_eq!(details["recovery"]["requires_intervention"], true);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'agent.wake.admitted' AND scope_id = ?",
        )
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap(),
        1
    );

    // A late Task disposition updates the same incident rather than creating
    // an execution-keyed duplicate, and its later recovery resolves it.
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let blocked = TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: current.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(
                serde_json::json!({
                    "type": "executor_failed",
                    "recovery_actions": ["reexecute", "cancel_task"]
                })
                .to_string(),
            )),
            blocked_json: Some(Some(
                serde_json::json!({
                    "reason": "late dispatcher disposition",
                    "kind": "executor_failed",
                    "execution_id": execution_id
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
    .unwrap();
    service.project_once(100).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM attention_projection
             WHERE attention_type = 'execution_failed' AND scope_id = ?",
        )
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'agent.wake.admitted' AND scope_id = ?",
        )
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap(),
        1,
        "late Task disposition must not create a second wake for the incident"
    );

    TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: blocked.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(None),
            blocked_json: Some(None),
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    service.project_once(100).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM attention_projection
             WHERE attention_type = 'execution_failed' AND scope_id = ?",
        )
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap(),
        "resolved"
    );
}

#[tokio::test]
async fn actionable_task_interruption_wakes_once_and_resolution_closes_attention() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "interruption-project").await;
    let task = project_task(&db, &project_id, "Recover actionable interruption").await;
    let execution_id = new_uuid_v4();
    let now = now_rfc3339();
    let blocked_json = serde_json::json!({
        "reason": "executor failed",
        "created_at": now,
        "kind": "executor_failed",
        "execution_id": execution_id
    })
    .to_string();
    let error_annotation = serde_json::json!({
        "type": "executor_failed",
        "recovery_actions": ["reexecute", "cancel_task"]
    })
    .to_string();
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: current.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(error_annotation)),
            blocked_json: Some(Some(blocked_json)),
            failed_json: Some(None),
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();

    let service = AttentionService::new(Arc::clone(&db));
    service.project_once(100).await.unwrap();

    let attention: (String, String, String) = sqlx::query_as(
        "SELECT attention_type, status, details_json FROM attention_projection
         WHERE attention_type = 'execution_failed' AND scope_id = ?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(attention.0, "execution_failed");
    assert_eq!(attention.1, "open");
    let details: serde_json::Value = serde_json::from_str(&attention.2).unwrap();
    assert_eq!(
        details["recovery"]["actions"],
        serde_json::json!(["reexecute", "cancel_task"])
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'agent.wake.admitted' AND scope_id = ?",
        )
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap(),
        1
    );

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: current.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(None),
            blocked_json: Some(None),
            failed_json: Some(None),
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    service.project_once(100).await.unwrap();

    let status: String = sqlx::query_scalar(
        "SELECT status FROM attention_projection
         WHERE attention_type = 'execution_failed' AND scope_id = ?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(status, "resolved");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'agent.wake.admitted' AND scope_id = ?",
        )
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap(),
        1,
        "resolution must not admit a second wake"
    );
}

#[tokio::test]
async fn completed_task_wakes_project_agent_until_readiness_is_reconciled() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "delivery-followup-project").await;
    let now = now_rfc3339();

    let task = project_task(&db, &project_id, "Delivered task").await;
    TaskRepo::update_status(
        &*db,
        db::UpdateTaskStatus {
            id: task.id.clone(),
            expected_version: task.version,
            status: "done".to_owned(),
            assignee_id: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent::task_transition(
            new_uuid_v4(),
            task.id.clone(),
            project_id.clone(),
            "in_progress",
            "done",
            Some("accept"),
            "system:workflow",
            "task completed",
            false,
            now,
            services::workflow::transition_event::transition_workflow_snapshot(
                &task,
                &default_workflow::default_workflow(),
                "in_progress",
                "done",
            )
            .unwrap(),
        ),
    )
    .await
    .unwrap();

    let service = AttentionService::new(Arc::clone(&db));
    service.project_once(100).await.unwrap();
    let attention: (String, String, String, Option<String>) = sqlx::query_as(
        "SELECT attention_type, status, recommended_action, identity_id
         FROM attention_projection WHERE scope_type = 'project' AND scope_id = ?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(attention.0, "delivery_followup");
    assert_eq!(attention.1, "open");
    assert_eq!(attention.2, "reconcile_delivery");
    assert_eq!(attention.3.as_deref(), Some(identity_id.as_str()));
    let admitted_identity: String = sqlx::query_scalar(
        "SELECT json_extract(payload_json, '$.identity_id')
         FROM domain_event WHERE event_type = 'agent.wake.admitted'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(admitted_identity, identity_id);

    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "milestone.readiness.evaluated".to_owned(),
            entity_type: "project_milestone".to_owned(),
            entity_id: new_uuid_v4(),
            actor_type: "agent".to_owned(),
            actor_id: Some(identity_id),
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 1,
            dedupe_key: Some(new_uuid_v4()),
            payload_json: r#"{"result":"blocked"}"#.to_owned(),
            created_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    service.project_once(100).await.unwrap();
    let resolved: String = sqlx::query_scalar(
        "SELECT status FROM attention_projection
         WHERE attention_type = 'delivery_followup' AND scope_id = ?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(resolved, "resolved");
}

#[tokio::test]
async fn expected_cancellation_is_audit_only_without_attention_or_wake() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let profile_id: String =
        sqlx::query_scalar("SELECT selected_profile_id FROM agent_identity WHERE id = ?")
            .bind(&identity_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    let project_id = new_uuid_v4();
    let now = now_rfc3339();
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.clone(),
            name: "stopped-execution-project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    sqlx::query(
        "UPDATE project_agent_binding
         SET identity_id = ?, profile_id = ?, state = 'active', wake_budget = 10,
             version = version + 1, updated_at = ?
         WHERE project_id = ? AND state = 'agent_setup_required'",
    )
    .bind(&identity_id)
    .bind(&profile_id)
    .bind(&now)
    .bind(&project_id)
    .execute(db.pool())
    .await
    .unwrap();
    let task = TaskRepo::create(
        &*db,
        CreateTask {
            id: new_uuid_v4(),
            project_id: project_id.clone(),
            repo_id: None,
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Recover interrupted delivery".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "in_progress".to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let execution_id = new_uuid_v4();
    ExecutionRepo::create(
        &*db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(identity_id.clone()),
            role: "coder".to_owned(),
            status: ExecutionStatus::Cancelled,
            stop_reason: Some(db::StopReason::UserCancelled),
            stopped_by: Some("user:api".to_owned()),
            resume_policy: Some(ResumePolicy::None),
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "execution.cancelled".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: task.id.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(new_uuid_v4()),
            payload_json: serde_json::json!({
                "execution_id": execution_id,
                "task_id": task.id,
                "role": "coder",
                "stop_reason": "user_cancelled",
                "error": "execution stopped for reassignment"
            })
            .to_string(),
            created_at: now,
        },
    )
    .await
    .unwrap();

    AttentionService::new(Arc::clone(&db))
        .project_once(100)
        .await
        .unwrap();

    let attention_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM attention_projection
         WHERE attention_type = 'execution_failed' AND scope_id = ?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    let admitted_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event
         WHERE event_type = 'agent.wake.admitted' AND scope_id = ?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(attention_count, 0);
    assert_eq!(admitted_count, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_wake_lease WHERE scope_id = ?",)
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
        0
    );
}

/// Seed a Project with `workflow_definition` and one Task parked in `review`,
/// then project its `task.transitioned` event. Returns the attention rows
/// materialized for the Project.
async fn review_attention_rows(db: &Arc<SqliteDb>, workflow_definition: &str) -> Vec<String> {
    let identity_id = new_uuid_v4();
    identity(db, &identity_id).await;
    let profile_id: String =
        sqlx::query_scalar("SELECT selected_profile_id FROM agent_identity WHERE id = ?")
            .bind(&identity_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    let project_id = new_uuid_v4();
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO project (
            id, name, settings, workflow_definition, owner_id, created_at, updated_at
         ) VALUES (?, 'review-wake-project', '{}', ?, NULL, ?, ?)",
    )
    .bind(&project_id)
    .bind(workflow_definition)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE project_agent_binding
         SET identity_id = ?, profile_id = ?, state = 'active', wake_budget = 10,
             version = version + 1, updated_at = ?
         WHERE project_id = ? AND state = 'agent_setup_required'",
    )
    .bind(&identity_id)
    .bind(&profile_id)
    .bind(&now)
    .bind(&project_id)
    .execute(db.pool())
    .await
    .unwrap();
    let task = TaskRepo::create(
        &**db,
        CreateTask {
            id: new_uuid_v4(),
            project_id: project_id.clone(),
            repo_id: None,
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Deliver the reviewed slice".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "review".to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let workflow =
        services::workflow::engine::WorkflowEngine::resolve_workflow(workflow_definition);
    let from = workflow
        .states
        .iter()
        .find(|state| state.kind == api_types::StateKind::Active)
        .unwrap();
    let snapshot = services::workflow::transition_event::transition_workflow_snapshot(
        &task, &workflow, &from.name, "review",
    )
    .unwrap();
    DomainEventRepo::append_event(
        &**db,
        CreateDomainEvent::task_transition(
            new_uuid_v4(),
            task.id.clone(),
            project_id.clone(),
            &from.name,
            "review",
            Some("accept"),
            "system:workflow",
            "review ready",
            false,
            now,
            snapshot,
        ),
    )
    .await
    .unwrap();
    AttentionService::new(Arc::clone(db))
        .project_once(100)
        .await
        .unwrap();
    sqlx::query_scalar(
        "SELECT attention_type FROM attention_projection
         WHERE scope_type = 'project' AND scope_id = ? ORDER BY updated_at",
    )
    .bind(&project_id)
    .fetch_all(db.pool())
    .await
    .unwrap()
}

#[tokio::test]
async fn review_ready_wakes_only_for_a_human_required_review_gate() {
    let db = database().await;
    // The default workflow's review gate is run by the reviewer Agent: the
    // Project Agent has nothing to decide, so no attention and no wake.
    let agent_reviewed =
        serde_json::to_string(&default_workflow::default_workflow()).expect("serialize workflow");
    assert!(
        review_attention_rows(&db, &agent_reviewed).await.is_empty(),
        "an agent-run review gate must not raise review_ready attention"
    );
    // The autonomous workflow's review gate is a user decision.
    let human_reviewed =
        serde_json::to_string(&default_autonomous_workflow::default_autonomous_workflow())
            .expect("serialize workflow");
    assert_eq!(
        review_attention_rows(&db, &human_reviewed).await,
        vec!["review_ready".to_owned()],
        "a human-required review gate must raise exactly one review_ready attention"
    );
}
