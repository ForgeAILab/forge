use crate::test_services as services;
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
            // Several wake fixtures stage more than one Running execution for
            // this identity on purpose (a superseded attempt beside its
            // replacement); a capacity of 1 makes the fixture itself
            // unbuildable. No test here asserts capacity behaviour.
            max_concurrent_tasks: 4,
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
    let project_id = new_uuid_v4();
    let now = now_rfc3339();
    let owner_id = new_uuid_v4();
    db::UserRepo::create_user(
        &**db,
        &db::User {
            id: owner_id.clone(),
            email: format!("{owner_id}@example.test"),
            password_hash: "test".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE agent_identity SET owner_id=? WHERE id=?")
        .bind(&owner_id)
        .bind(identity_id)
        .execute(db.pool())
        .await
        .unwrap();
    ProjectRepo::create(
        &**db,
        CreateProject {
            id: project_id.clone(),
            name: name.to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(owner_id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let version:i64=sqlx::query_scalar("SELECT version FROM project_agent_binding WHERE project_id=? AND state='agent_setup_required'").bind(&project_id).fetch_one(db.pool()).await.unwrap();
    services::AgentChatService::new(db.clone())
        .set_project_binding(services::SetProjectAgentBindingInput {
            actor_user_id: owner_id,
            project_id: project_id.clone(),
            identity_id: Some(identity_id.to_owned()),
            state: "active".to_owned(),
            autonomy_policy_json: "{}".to_owned(),
            permission_ceiling_json: "{}".to_owned(),
            subscriptions_json: "[]".to_owned(),
            wake_budget: 10,
            expected_version: Some(version),
            replacement_reason: None,
        })
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
    let tasks =
        services::TaskService::new_for_test(Arc::clone(&db), Arc::new(events::EventBus::default()));
    let attention = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );

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
                .stop_execution(&execution.id, "Stop this work".to_owned())
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
        assert!(annotation.get("recovery_actions").is_none());
        let offers = tasks
            .task_action_offers(
                &task.id,
                &api_types::Actor::user(api_types::UserActionSource::Test),
            )
            .await
            .unwrap();
        assert!(offers
            .available_actions
            .iter()
            .any(|offer| offer.action.verb() == "cancel"));
        let run = attention.project_once(100).await.unwrap();
        assert_eq!(
            run.processed_events, run.claimed_events,
            "intentional stops must not hold the event cursor for orphan grace"
        );
        let payload: String = sqlx::query_scalar("SELECT payload_json FROM domain_event WHERE event_type = 'task.interruption_changed' AND entity_id = ? ORDER BY sequence DESC LIMIT 1")
            .bind(&task.id).fetch_one(db.pool()).await.unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&payload).unwrap()["material_blocker"]
                ["requires_intervention"],
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
    let attention = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
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
        sqlx::query("UPDATE agent_chat SET status = 'agent_setup_required' WHERE project_id = ?")
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
        let attention = AttentionService::new(
            Arc::clone(&db),
            Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
        );
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
                TaskRepo::mutate_metadata(
                    &*db,
                    &task.id,
                    None,
                    vec![db::TaskMetadataMutation::Set {
                        key: "deferred_dispatch".to_owned(),
                        value: serde_json::json!({
                            "not_before": (Utc::now() + Duration::minutes(1)).to_rfc3339(),
                            "reason": "retry scheduled", "target_state": "in_progress"
                        }),
                    }],
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
                db.check_task_conditions_of(std::slice::from_ref(&task.id))
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        sqlx::query("UPDATE agent_chat SET status = 'ready' WHERE project_id = ?")
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
    let service = AttentionService::new(
        db.clone(),
        Arc::new(services::WakeTurnConsumer::new(db.clone())),
    );
    assert!(matches!(
        service
            .admit_wake(request(&identity_id, "missing-incident"))
            .await
            .unwrap(),
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::ResolvedIncident
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
async fn identical_suppressed_wake_decision_is_deduped() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
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
            payload_json: r#"{"status":"failed","failure_class":{"kind":"configuration"}}"#
                .to_owned(),
            created_at: now,
        },
    )
    .await
    .unwrap();

    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
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
    let project_id = configured_project(&db, &old_identity, "binding-replacement-project").await;
    let now = now_rfc3339();
    let owner: String = sqlx::query_scalar("SELECT owner_id FROM project WHERE id=?")
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE agent_identity SET owner_id=? WHERE id=?")
        .bind(&owner)
        .bind(&new_identity)
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
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    service.project_once(100).await.unwrap();
    let admitted_identity: String = sqlx::query_scalar(
        "SELECT json_extract(payload_json, '$.identity_id') FROM domain_event
         WHERE event_type = 'agent.wake.admitted'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(admitted_identity, old_identity);

    let version: i64 = sqlx::query_scalar(
        "SELECT version FROM project_agent_binding WHERE project_id=? AND state='active'",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    services::AgentChatService::new(db.clone())
        .set_project_binding(services::SetProjectAgentBindingInput {
            actor_user_id: owner,
            project_id: project_id.clone(),
            identity_id: Some(new_identity),
            state: "active".to_owned(),
            autonomy_policy_json: "{}".to_owned(),
            permission_ceiling_json: "{}".to_owned(),
            subscriptions_json: "[]".to_owned(),
            wake_budget: 10,
            expected_version: Some(version),
            replacement_reason: Some("binding replacement test".to_owned()),
        })
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

    let result = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    )
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

    let result = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    )
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
async fn projection_health_uses_buffered_checkpoints_and_idle_polls_do_not_write() {
    let db = database().await;
    for sequence in 0..100 {
        DomainEventRepo::append_event(
            &*db,
            CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: "test.health_progress".to_owned(),
                entity_type: "task".to_owned(),
                entity_id: format!("health-task-{sequence}"),
                actor_type: "system".to_owned(),
                actor_id: None,
                scope_type: "account".to_owned(),
                scope_id: "health-account".to_owned(),
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
    }
    let service = Arc::new(AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    ));
    let runtime =
        services::worker_runtime::WorkerRuntime::new(Arc::clone(&db), Arc::clone(&service));
    assert_eq!(
        runtime.run_once(100).await.unwrap(),
        0,
        "ignored events have no per-event write"
    );
    tokio::time::sleep(std::time::Duration::from_millis(5100)).await;
    runtime.run_once(100).await.unwrap(); // buffered scan checkpoint
    let health = service.consumer_health().await.unwrap().unwrap();
    assert!(!health.stale);
    assert_eq!(health.last_sequence, 100);
    sqlx::query("CREATE TRIGGER reject_health_write BEFORE UPDATE ON worker_health BEGIN SELECT RAISE(ABORT, 'unexpected health write'); END")
        .execute(db.pool()).await.unwrap();
    for _ in 0..10 {
        assert_eq!(runtime.run_once(100).await.unwrap(), 0);
    }
    let idle_health = service.consumer_health().await.unwrap().unwrap();
    assert_eq!(idle_health.updated_at, health.updated_at);
    assert!(!health.stale);
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

    let service = std::sync::Arc::new(AttentionService::new(
        std::sync::Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(std::sync::Arc::clone(&db))),
    ));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = std::sync::Arc::clone(&service).start(shutdown_rx);
    let mut health = None;
    for _ in 0..100 {
        health = service.consumer_health().await.unwrap();
        if health
            .as_ref()
            .is_some_and(|value| value.last_sequence >= 1)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(health.is_some_and(|value| value.last_sequence >= 1));

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
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
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

    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
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

    AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    )
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

    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
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
async fn review_owner_parks_stay_visible_without_wakes_or_escalations() {
    for initially_paused in [false, true] {
        let db = database().await;
        let identity_id = new_uuid_v4();
        identity(&db, &identity_id).await;
        let project_id = configured_project(&db, &identity_id, "owner-only-review").await;
        sqlx::query("UPDATE agent_identity SET paused=? WHERE id=?")
            .bind(initially_paused)
            .bind(&identity_id)
            .execute(db.pool())
            .await
            .unwrap();
        let task = project_task(&db, &project_id, "Owner must decide").await;
        let now = now_rfc3339();
        let annotation = serde_json::json!({
            "type": "review_needs_owner", "message": "repeated finding F1"
        });
        let condition = serde_json::json!({
            "kind": "review_needs_owner", "reason": "repeated finding F1", "created_at": now
        });
        TaskRepo::update_status(
            &*db,
            db::UpdateTaskStatus {
                id: task.id.clone(),
                expected_version: task.version,
                status: "review".into(),
                assignee_id: None,
                error_annotation: Some(Some(annotation.to_string())),
                blocked_json: Some(Some(condition.to_string())),
                failed_json: Some(None),
                updated_at: now.clone(),
            },
        )
        .await
        .unwrap();
        let service = || {
            AttentionService::new(
                Arc::clone(&db),
                Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
            )
        };
        service().project_once(100).await.unwrap();
        let attention_id: String = sqlx::query_scalar(
            "SELECT id FROM attention_projection
         WHERE attention_type='review_needs_owner' AND scope_id=? AND status='open'",
        )
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        let owner_park = db.get_attention(&attention_id).await.unwrap().unwrap();
        assert_eq!(
            owner_park.summary,
            "Review needs the owner: repeated finding F1"
        );
        // Fresh service instances exercise startup sweeps as well as event projection.
        for _ in 0..2 {
            assert_eq!(service().sweep_once_at(&now_rfc3339()).await.unwrap(), 0);
            service().project_once(100).await.unwrap();
        }
        for table in [
            "agent_chat_turn_job",
            "agent_wake_budget_window",
            "agent_wake_lease",
        ] {
            let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
                .fetch_one(db.pool())
                .await
                .unwrap();
            assert_eq!(count, 0, "owner review parks must not create {table}");
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM domain_event WHERE event_type LIKE 'agent.wake.%'"
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            0,
            "owner-only work needs neither an Agent wake nor a setup decision"
        );

        // An ordinary blocker beside the owner park still wakes and escalates normally.
        sqlx::query("UPDATE agent_identity SET paused=0 WHERE id=?")
            .bind(&identity_id)
            .execute(db.pool())
            .await
            .unwrap();
        let failure = project_task(&db, &project_id, "Recover executor failure").await;
        let annotation = serde_json::json!({"type": "executor_failed"});
        let condition = serde_json::json!({
            "kind": "executor_failed", "reason": "executor failed", "created_at": now
        });
        TaskRepo::update_status(
            &*db,
            db::UpdateTaskStatus {
                id: failure.id.clone(),
                expected_version: failure.version,
                status: "in_progress".into(),
                assignee_id: None,
                error_annotation: Some(Some(annotation.to_string())),
                blocked_json: Some(Some(condition.to_string())),
                failed_json: Some(None),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
        service().project_once(100).await.unwrap();
        let linked: Vec<String> = sqlx::query_scalar(
            "SELECT a.attention_type FROM agent_wake_blocker b
         JOIN attention_projection a ON a.id=b.attention_id",
        )
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(linked, vec!["execution_failed"]);
        let turn_id: String = sqlx::query_scalar("SELECT id FROM agent_chat_turn_job")
            .fetch_one(db.pool())
            .await
            .unwrap();
        // A turn admitted before this policy changed can still finish after an
        // upgrade. Its old owner-park link must not duplicate the owner request.
        sqlx::query(
            "INSERT INTO agent_wake_blocker
                    (attention_id,incident_digest,turn_job_id,admitted_at) VALUES (?,?,?,?)",
        )
        .bind(&attention_id)
        .bind(services::wake_attention_incident_digest(&owner_park))
        .bind(&turn_id)
        .bind(&now)
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "UPDATE agent_chat_turn_job SET status='succeeded',version=version+1 WHERE id=?",
        )
        .bind(&turn_id)
        .execute(db.pool())
        .await
        .unwrap();
        services::project_escalation::ProjectEscalationService::new(Arc::clone(&db))
            .after_wake_turn(&turn_id)
            .await
            .unwrap();
        service().project_once(100).await.unwrap();
        let needs: Vec<String> = sqlx::query_scalar(
            "SELECT details_json FROM attention_projection
         WHERE attention_type='human_input_required' AND status='open'",
        )
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(needs.len(), 1, "the ordinary blocker still escalates");
        assert!(needs[0].contains(&failure.id));
        assert!(
            !needs[0].contains(&task.id),
            "the owner park must not be escalated again"
        );
        assert_eq!(
            db.get_attention(&attention_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "open"
        );
    }
}

#[tokio::test]
async fn a_task_leaving_its_owner_park_for_an_agent_cause_wakes_the_project_agent() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "owner-park-exit").await;
    let task = project_task(&db, &project_id, "Leaves the owner park").await;
    let now = now_rfc3339();
    let park = |kind: &str, status: &str, version: i64| db::UpdateTaskStatus {
        id: task.id.clone(),
        expected_version: version,
        status: status.into(),
        assignee_id: None,
        error_annotation: Some(Some(serde_json::json!({"type": kind}).to_string())),
        blocked_json: Some(Some(
            serde_json::json!({"kind": kind, "reason": kind, "created_at": now}).to_string(),
        )),
        failed_json: Some(None),
        updated_at: now_rfc3339(),
    };
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    TaskRepo::update_status(&*db, park("review_needs_owner", "review", task.version))
        .await
        .unwrap();
    service.project_once(100).await.unwrap();
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 0);
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    TaskRepo::update_status(
        &*db,
        park("executor_failed", "in_progress", current.version),
    )
    .await
    .unwrap();
    service.project_once(100).await.unwrap();
    service.sweep_once_at(&now_rfc3339()).await.unwrap();

    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT attention_type, status FROM attention_projection
         WHERE attention_type IN ('review_needs_owner', 'execution_failed')
         ORDER BY attention_type",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(
        rows,
        vec![
            ("execution_failed".to_owned(), "open".to_owned()),
            ("review_needs_owner".to_owned(), "resolved".to_owned()),
        ]
    );
    let linked: Vec<String> = sqlx::query_scalar(
        "SELECT a.attention_type FROM agent_wake_blocker b
         JOIN attention_projection a ON a.id=b.attention_id",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(
        linked,
        vec!["execution_failed"],
        "the Agent-handled cause that replaced the owner park must wake the Project Agent"
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

    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
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
    let task_service =
        services::TaskService::new_for_test(Arc::clone(&db), Arc::new(events::EventBus::new(32)));
    let expected = task_service
        .task_action_offers(&task.id, &api_types::Actor::agent(&identity_id))
        .await
        .unwrap()
        .available_actions;
    assert_eq!(
        details["recovery"]["actions"],
        serde_json::to_value(&expected).unwrap()
    );
    assert_eq!(
        expected
            .iter()
            .map(|offer| offer.action.verb())
            .collect::<Vec<_>>(),
        vec!["cancel", "restart"]
    );
    assert!(expected.iter().all(|offer| offer
        .parameters
        .iter()
        .any(|parameter| parameter.name == "reason" && parameter.required)));
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

    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
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

    AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    )
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
    AttentionService::new(
        Arc::clone(db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(db))),
    )
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

#[tokio::test]
async fn audit_skipped_activity_is_not_stale() {
    let db = database().await;
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    service.project_once(1).await.unwrap(); // initialize the worker row
    for _ in 0..5 {
        append_attention_event(&db, "ignored.kind", "t", "p", serde_json::json!({})).await;
    }
    assert_eq!(service.project_once(100).await.unwrap().processed_events, 0);
    tokio::time::sleep(std::time::Duration::from_millis(5100)).await;
    service.project_once(100).await.unwrap(); // lazy scan checkpoint
    let health = service.consumer_health().await.unwrap().unwrap();
    println!(
        "health: checkpoint={} success={:?} stale={}",
        health.last_sequence, health.last_success_at, health.stale
    );
    // Whether or not the lazy checkpoint has been written by a fresh runtime,
    // the events were consumed without ever recording a success time.
    assert!(health.last_success_at.is_none());
    assert!(
        !health.stale,
        "skipped activity must not make the consumer stale"
    );
}

#[tokio::test]
async fn audit_in_window_budget_exhaustion_does_not_publish_autonomy_stall() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "stall-project").await;
    let task = project_task(&db, &project_id, "Recover actionable interruption").await;
    let now = now_rfc3339();
    let blocked_json = serde_json::json!({
        "reason": "executor failed",
        "created_at": now,
        "kind": "executor_failed",
        "execution_id": new_uuid_v4()
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
    // The hourly window is already full (budget 10, 10 admitted, in window).
    sqlx::query(
        "INSERT INTO agent_wake_budget_window (identity_id, scope_type, scope_id, category, window_started_at, window_seconds, admitted_count, version, updated_at)
         VALUES (?, 'project', ?, 'blocker', ?, 3600, 4, 1, ?)",
    )
    .bind(&identity_id)
    .bind(&project_id)
    .bind(now_rfc3339())
    .bind(now_rfc3339())
    .execute(db.pool())
    .await
    .unwrap();
    let bus = Arc::new(events::EventBus::new(64));
    let mut rx = bus.subscribe();
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    )
    .with_event_bus(Arc::clone(&bus));
    service.project_once(100).await.unwrap();
    let suppressed: Vec<String> = sqlx::query_scalar(
        "SELECT payload_json FROM domain_event WHERE event_type = 'agent.wake.suppressed'",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    println!("AUDIT suppressed decisions: {suppressed:?}");
    let mut stalls = 0;
    while let Ok(event) = rx.try_recv() {
        println!("AUDIT bus event: {}", event.event_type);
        if event.event_type == "project.autonomy_stalled" {
            stalls += 1;
        }
    }
    println!("AUDIT stalls published for in-window exhaustion: {stalls}");
    assert!(suppressed.iter().any(|p| p.contains("budget_exhausted")));
    assert_eq!(
        stalls, 0,
        "full hourly window must not send a stall notification"
    );
}

#[tokio::test]
async fn audit_idle_and_new_upgrade_health_are_not_stale_and_stalled_backlog_is() {
    let db = database().await;
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    service.project_once(1).await.unwrap();
    sqlx::query("UPDATE worker_health SET last_success_at = '2000-01-01T00:00:00Z' WHERE worker_name = 'attention_projection'").execute(db.pool()).await.unwrap();
    assert!(!service.consumer_health().await.unwrap().unwrap().stale);
    append_attention_event_at(
        &db,
        "ignored.kind",
        "t",
        "p",
        serde_json::json!({}),
        "2000-01-01T00:00:00Z",
    )
    .await;
    sqlx::query("UPDATE event_consumer_cursor SET updated_at = '2000-01-01T00:00:00Z' WHERE consumer_name = 'attention_projection'").execute(db.pool()).await.unwrap();
    assert!(
        !service.consumer_health().await.unwrap().unwrap().stale,
        "initialization grace applies to upgraded checkpoints"
    );
    sqlx::query("UPDATE worker_health SET created_at = '2000-01-01T00:00:00Z' WHERE worker_name = 'attention_projection'").execute(db.pool()).await.unwrap();
    assert!(
        service.consumer_health().await.unwrap().unwrap().stale,
        "old pending event plus old checkpoint is stalled"
    );
    sqlx::query("UPDATE event_consumer_cursor SET last_sequence = (SELECT MAX(sequence) FROM domain_event) WHERE consumer_name = 'attention_projection'").execute(db.pool()).await.unwrap();
    assert!(
        !service.consumer_health().await.unwrap().unwrap().stale,
        "caught-up idle consumer is healthy even with old success timestamps"
    );
}

#[tokio::test]
async fn audit_health_reports_worker_error_kind_and_message() {
    use services::worker_runtime::{HealthErrorKind, WorkItem, WorkerHealth};

    let db = database().await;
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    service.project_once(1).await.unwrap();
    let health = WorkerHealth::new(Arc::clone(&db), "attention_projection");
    for kind in ["failure", "transient", "terminal"] {
        let message = format!("diagnostic for {kind}");
        health
            .report_classified_error(HealthErrorKind::Tick, kind, &message)
            .await
            .unwrap();
        let view = service.consumer_health().await.unwrap().unwrap();
        assert_eq!(view.last_error_code.as_deref(), Some(kind));
        assert_eq!(view.last_error_message.as_deref(), Some(message.as_str()));
        assert!(
            !view.stale,
            "an idle worker is not stale even after an error"
        );
    }
    health
        .clear_error_if_set(HealthErrorKind::Tick)
        .await
        .unwrap();
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    health
        .failure_in_tx(
            &mut tx,
            WorkItem {
                source_key: "test-terminal",
                item_type: "test",
            },
            db::RetryPolicy::default(),
            "deterministic projection rejection",
            true,
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let view = service.consumer_health().await.unwrap().unwrap();
    assert_eq!(view.last_error_code.as_deref(), Some("terminal"));
    assert_eq!(
        view.last_error_message.as_deref(),
        Some("deterministic projection rejection")
    );
}

#[tokio::test]
async fn audit_zero_budget_chat_stall_uses_project_scope() {
    let db = database().await;
    let id = new_uuid_v4();
    identity(&db, &id).await;
    let project = configured_project(&db, &id, "chat-budget").await;
    let chat = services::AgentChatService::new(Arc::clone(&db))
        .ensure_project_chat(&project)
        .await
        .unwrap();
    sqlx::query("UPDATE agent_chat SET status = 'ready' WHERE id = ?")
        .bind(&chat.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE project_agent_binding SET wake_budget = 0 WHERE project_id = ?")
        .bind(&project)
        .execute(db.pool())
        .await
        .unwrap();
    append_attention_event(
        &db,
        "validation.failed",
        "task-budget",
        &project,
        serde_json::json!({}),
    )
    .await;
    let source: String = sqlx::query_scalar("SELECT id FROM domain_event WHERE entity_id = 'task-budget' ORDER BY sequence DESC LIMIT 1").fetch_one(db.pool()).await.unwrap();
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO attention_projection (id, attention_type, scope_type, scope_id, identity_id, source_event_id, priority, status, summary, details_json, dedupe_key, occurred_at, updated_at) VALUES (?, 'validation_failed', 'project', ?, ?, ?, 1, 'open', 'Needs work', '{}', 'chat-budget-incident', ?, ?)")
        .bind(new_uuid_v4()).bind(&project).bind(&id).bind(source).bind(now_rfc3339()).bind(now_rfc3339()).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let bus = Arc::new(events::EventBus::new(16));
    let mut rx = bus.subscribe();
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    )
    .with_event_bus(bus);
    let result = service
        .admit_wake(services::WakeAdmissionRequest {
            identity_id: id,
            scope_type: "agent_chat".into(),
            scope_id: chat.id,
            incident_key: "chat-budget-incident".into(),
            lease_owner: "test".into(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            caused_by_identity_id: None,
            reaction_depth: 0,
            now: now_rfc3339(),
            lease_seconds: 60,
            cooldown_seconds: 300,
        })
        .await
        .unwrap();
    assert!(matches!(
        result,
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::BudgetExhausted
        }
    ));
    let requests: Vec<(String, String)> = sqlx::query_as("SELECT scope_id, payload_json FROM domain_event WHERE event_type = 'notification.requested'").fetch_all(db.pool()).await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].0, project);
    let request: serde_json::Value = serde_json::from_str(&requests[0].1).unwrap();
    assert_eq!(request["event_type"], "project.autonomy_stalled");
    assert_eq!(request["project_id"], project);
    // Delivery reads only the committed request, independently of the live hint.
    let notifications = Arc::new(services::NotificationService::new(
        Arc::clone(&db),
        Arc::new(events::EventBus::new(1)),
    ));
    services::worker_runtime::WorkerRuntime::new(Arc::clone(&db), notifications)
        .run_once(100)
        .await
        .unwrap();
    let inbox_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM notification WHERE project_id = ? AND event_type = 'project.autonomy_stalled'").bind(&project).fetch_one(db.pool()).await.unwrap();
    assert_eq!(inbox_count, 1);
    let event = rx.try_recv().unwrap();
    assert!(
        matches!(event.context, events::EventContext::ProjectAutonomyStalled { project_id, .. } if project_id == project)
    );
}

#[tokio::test]
async fn audit_admitted_source_replay_is_a_noop() {
    let db = database().await;
    let id = new_uuid_v4();
    identity(&db, &id).await;
    let project = configured_project(&db, &id, "replay-noop").await;
    append_attention_event(
        &db,
        "validation.failed",
        "task-replay",
        &project,
        serde_json::json!({}),
    )
    .await;
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    service.project_once(100).await.unwrap();
    let before: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT COUNT(*) FROM domain_event WHERE event_type LIKE 'agent.wake.%'), (SELECT version FROM attention_projection LIMIT 1), (SELECT admitted_count FROM agent_wake_budget_window LIMIT 1)").fetch_one(db.pool()).await.unwrap();
    sqlx::query("UPDATE event_consumer_cursor SET last_sequence = 0 WHERE consumer_name = 'attention_projection'").execute(db.pool()).await.unwrap();
    service.project_once(100).await.unwrap();
    let after: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT COUNT(*) FROM domain_event WHERE event_type LIKE 'agent.wake.%'), (SELECT version FROM attention_projection LIMIT 1), (SELECT admitted_count FROM agent_wake_budget_window LIMIT 1)").fetch_one(db.pool()).await.unwrap();
    assert_eq!(
        before, after,
        "already admitted source does not increment budget/version or append suppression"
    );
}

#[tokio::test]
async fn audit_lost_wake_lease_cas_suppresses_without_charging_budget() {
    let db = database().await;
    let id = new_uuid_v4();
    identity(&db, &id).await;
    let project = configured_project(&db, &id, "lease-cas").await;
    let now = now_rfc3339();
    let event = db
        .append_event(CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "test.incident".to_owned(),
            entity_type: "project".to_owned(),
            entity_id: project.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project.clone(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: None,
            payload_json: "{}".to_owned(),
            created_at: now.clone(),
        })
        .await
        .unwrap();
    db.insert_attention(CreateAttentionProjection {
        id: new_uuid_v4(),
        attention_type: "validation_failed".to_owned(),
        scope_type: "project".to_owned(),
        scope_id: project.clone(),
        identity_id: Some(id.clone()),
        source_event_id: event.id,
        priority: 80,
        status: "open".to_owned(),
        summary: "CAS loss".to_owned(),
        details_json: "{}".to_owned(),
        dedupe_key: "cas-loss".to_owned(),
        occurred_at: now.clone(),
        updated_at: now,
        acknowledged_at: None,
        snoozed_until: None,
        resolved_at: None,
        updated_by_user_id: None,
        recommended_action: "inspect".to_owned(),
        source_sequence: Some(event.sequence),
    })
    .await
    .unwrap();
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    sqlx::raw_sql("CREATE TRIGGER lose_wake_lease BEFORE INSERT ON agent_wake_lease BEGIN SELECT RAISE(IGNORE); END;").execute(db.pool()).await.unwrap();
    let mut input = request(&id, "cas-loss");
    input.scope_type = "project".to_owned();
    input.scope_id = project;
    let result = service.admit_wake(input).await.unwrap();
    assert!(matches!(
        result,
        WakeAdmissionResult::Suppressed {
            reason: WakeSuppressionReason::DuplicateIncident
        }
    ));
    let budgets: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_wake_budget_window")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(budgets, 0);
}

#[tokio::test]
async fn conflict_hotspots_project_per_path_reopen_and_keep_wake_rules() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "conflict-hotspots").await;
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    let mut first_id: Option<String> = None;
    for (path, count) in [("src/共有.rs", 3), ("src/共有.rs", 4), ("src/other.rs", 3)] {
        if count == 4 {
            let current = db
                .get_attention(first_id.as_ref().unwrap())
                .await
                .unwrap()
                .unwrap();
            db.update_attention_lifecycle(UpdateAttentionLifecycle {
                id: current.id,
                expected_version: current.version,
                status: "resolved".into(),
                acknowledged_at: None,
                snoozed_until: Some(None),
                resolved_at: Some(Some(now_rfc3339())),
                updated_by_user_id: None,
                updated_at: now_rfc3339(),
            })
            .await
            .unwrap();
        }
        db.append_event(CreateDomainEvent {
            id: new_uuid_v4(), event_type: "project.conflict_hotspot.detected".into(),
            entity_type: "project".into(), entity_id: project_id.clone(),
            actor_type: "system".into(), actor_id: Some("conflict-hotspots".into()),
            scope_type: "project".into(), scope_id: project_id.clone(),
            correlation_id: new_uuid_v4(), causation_id: None, causation_depth: 0,
            dedupe_key: Some(new_uuid_v4()),
            payload_json: serde_json::json!({"project_id": project_id, "path": path,
                "task_ids": ["task-3", "task-2", "task-1"], "handoff_count": count, "window_days": 7}).to_string(),
            created_at: now_rfc3339(),
        }).await.unwrap();
        service.project_once(100).await.unwrap();
        let rows = sqlx::query_as::<_, (String, String, String, String, String, i64, String, i64)>(
            "SELECT id, scope_type, scope_id, summary, recommended_action, priority, details_json, version
             FROM attention_projection WHERE attention_type = 'conflict_hotspot' ORDER BY source_sequence",
        ).fetch_all(db.pool()).await.unwrap();
        let item = rows.last().unwrap();
        assert_eq!(item.1, "project");
        assert_eq!(item.2, project_id);
        assert_eq!(
            item.3,
            format!("{path} conflicted in {count} Tasks this week")
        );
        assert_eq!(item.4, "split_hotspot");
        assert_eq!(item.5, 60);
        let details: serde_json::Value = serde_json::from_str(&item.6).unwrap();
        assert_eq!(details["conflict_hotspot"]["path"], path);
        if path == "src/共有.rs" {
            assert_eq!(rows.len(), 1);
            match &first_id {
                None => first_id = Some(item.0.clone()),
                Some(first) => {
                    assert_eq!(&item.0, first);
                    assert!(item.7 > 1);
                }
            }
        } else {
            assert_eq!(rows.len(), 2);
            assert_ne!(Some(&item.0), first_id.as_ref());
        }
        let projection = db.get_attention(&item.0).await.unwrap().unwrap();
        assert_eq!(projection.status, "open");
        assert!(projection.resolved_at.is_none());
        assert_eq!(
            services::attention_service::attention_item(projection)
                .unwrap()
                .category,
            api_types::AttentionCategory::ConflictHotspot
        );
    }
    let admitted: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'agent.wake.admitted'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(admitted, 2); // A later episode still obeys the same path's live lease/cooldown.
    let ineligible: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_event WHERE event_type = 'agent.wake.suppressed' AND json_extract(payload_json, '$.reason') = 'ineligible_scope'")
        .fetch_one(db.pool()).await.unwrap();
    assert_eq!(ineligible, 0);
}

/// A real stage-three-shaped condition/event, an already open incident and a
/// configured responder cross the migration without a new incident or wake.
#[tokio::test]
async fn condition_reader_upgrade_keeps_open_incident_and_wake_identity() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "Condition cutover").await;
    let task = project_task(&db, &project_id, "Unchanged blocker").await;
    let reason = "owner must repair the workspace";
    let interrupted =
        serde_json::json!({"kind":"workspace_error","reason":reason,"created_at":now_rfc3339()})
            .to_string();
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    sqlx::query("UPDATE task SET blocked_json=? WHERE id=?")
        .bind(&interrupted)
        .bind(&task.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    db.sync_condition_in_tx(&mut tx, &task.id).await.unwrap();
    // The typed presentation did not exist on the stage-three base.
    let raw: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
        .bind(&task.id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let mut old: serde_json::Value = serde_json::from_str(&raw).unwrap();
    old["evidence"]
        .as_object_mut()
        .unwrap()
        .remove("presentation");
    sqlx::query("UPDATE task SET condition_json=? WHERE id=?")
        .bind(old.to_string())
        .bind(&task.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    append_attention_event(&db,"task.interruption_changed",&task.id,&project_id,serde_json::json!({"task_id":task.id,"task_version":task.version,"task_status":task.status,"requires_intervention":true,"interruption":{"source":"blocked","kind":"workspace_error","reason":reason,"execution_id":"historical-reporter"}})).await;
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    service.project_once(100).await.unwrap();
    let incident_id: String = sqlx::query_scalar("SELECT id FROM attention_projection WHERE attention_type='execution_failed' AND scope_id=?").bind(&project_id).fetch_one(db.pool()).await.unwrap();
    // The incident row as `777b5f1f` stored it. That server copied the
    // event's `interruption` into the incident as written, reporting
    // execution included; this build strips delivery metadata on the way in.
    // That one field is the only part of the stored incident the two builds
    // write differently for this event, so put it back as the old server
    // left it and take the digest from that row.
    sqlx::query("UPDATE attention_projection SET details_json=json_set(details_json,'$.interruption',json(?)) WHERE id=?")
        .bind(serde_json::json!({"source":"blocked","kind":"workspace_error","reason":reason,"execution_id":"historical-reporter"}).to_string())
        .bind(&incident_id)
        .execute(db.pool())
        .await
        .unwrap();
    let stored_details: String =
        sqlx::query_scalar("SELECT details_json FROM attention_projection WHERE id=?")
            .bind(&incident_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert!(stored_details.contains("historical-reporter"));
    let before = AttentionRepo::get_attention(&*db, &incident_id)
        .await
        .unwrap()
        .unwrap();
    let digest = db::canonical_attention_incident_digest(&before);
    let wakes_before: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM domain_event WHERE event_type='agent.wake.admitted' AND scope_id=?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        wakes_before, 1,
        "the responder is configured and the old blocker admitted a wake"
    );
    // Reinstall just the predecessor's projection trigger/index, then replay
    // the new migration through the production runner. Task and event data stay.
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    sqlx::query("DELETE FROM _migration WHERE version=202610070507")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DROP INDEX idx_task_condition_retry_project")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    run_migrations(db.pool()).await.unwrap();
    let condition = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap()
        .condition;
    assert!(db::material_blocker(&condition).requires_intervention);
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    DomainEventRepo::append_event(&*db, CreateDomainEvent::task_interruption_changed(&current))
        .await
        .unwrap();
    service.project_once(100).await.unwrap();
    let after = AttentionRepo::get_attention(&*db, &incident_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(digest, db::canonical_attention_incident_digest(&after));
    assert_eq!(before.id, after.id);
    let incidents: i64 = sqlx::query_scalar("SELECT count(*) FROM attention_projection WHERE attention_type='execution_failed' AND scope_id=?").bind(&project_id).fetch_one(db.pool()).await.unwrap();
    assert_eq!(incidents, 1);
    let wakes_after: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM domain_event WHERE event_type='agent.wake.admitted' AND scope_id=?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        wakes_after, wakes_before,
        "migration must not re-arm a consumed incident"
    );
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    TaskRepo::update(&*db,db::UpdateTask {id:task.id.clone(),expected_version:current.version,title:None,description:None,priority:None,merge_config:None,plan:None,error_annotation:Some(None),blocked_json:Some(Some(serde_json::json!({"kind":"workspace_error","reason":"a different workspace blocker","created_at":now_rfc3339()}).to_string())),failed_json:Some(None),task_state_config:None,parent_task_id:None,updated_at:now_rfc3339()}).await.unwrap();
    service.project_once(100).await.unwrap();
    let changed = AttentionRepo::get_attention(&*db, &incident_id)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(digest, db::canonical_attention_incident_digest(&changed));
    let material_events: i64 = sqlx::query_scalar("SELECT count(*) FROM domain_event WHERE event_type='task.interruption_changed' AND entity_id=?").bind(&task.id).fetch_one(db.pool()).await.unwrap();
    assert_eq!(
        material_events, 3,
        "one historical event, one unchanged typed replay, exactly one real blocker change"
    );
}

/// Plan 3.1 stage 5a, Attention continuity. The starting state is what the
/// deployed build (`9d9b228f`) leaves in the database, not what this build
/// would write:
///
/// - the `task.interruption_changed` event in that build's payload shape
///   (`crates/db/src/repository.rs:976-982` there: no `condition`, no
///   `material_blocker`);
/// - the open incident row with the `details_json` that build's projector
///   stored (`crates/services/src/attention_service.rs:2562-2584` there),
///   hard-coded below member by member;
/// - the wake rows and their stored incident digest. `db::sqlite::agent_wake`
///   is unchanged since that commit, and the digest stored there is checked
///   against that build's digest algorithm (`crates/db/src/models.rs:926-1003`
///   there), restated here so it does not share code with this build.
///
/// This build then starts and runs its first projection and wake sweep.
#[tokio::test]
async fn incident_stored_by_the_deployed_build_survives_this_builds_first_pass() {
    use serde_json::{json, Value};
    use sha2::{Digest, Sha256};

    fn deployed_strip(value: &mut Value) {
        match value {
            Value::Object(object) => {
                for key in [
                    "source_event_id",
                    "source_event_type",
                    "source_sequence",
                    "task_version",
                    "created_at",
                    "updated_at",
                    "detected_at",
                    "last_checked_at",
                    "next_check_at",
                    "execution_id",
                ] {
                    object.remove(key);
                }
                object.values_mut().for_each(deployed_strip);
            }
            Value::Array(array) => array.iter_mut().for_each(deployed_strip),
            _ => {}
        }
    }
    fn sha(value: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(value.as_bytes());
        hasher.update([0]);
        format!("sha256:{}", hex::encode(hasher.finalize()))
    }
    /// `canonical_attention_incident_digest` as `9d9b228f` computes it.
    async fn deployed_digest(db: &SqliteDb, incident_id: &str) -> String {
        let (kind, scope_type, scope_id, status, details, action): (String, String, String, String, String, String) =
            sqlx::query_as("SELECT attention_type,scope_type,scope_id,status,details_json,recommended_action FROM attention_projection WHERE id=?")
                .bind(incident_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        let bounded: String = details.chars().take(8_192).collect();
        let canonical = serde_json::from_str::<Value>(&bounded)
            .map(|mut value| {
                deployed_strip(&mut value);
                if let Some(task) = value.get_mut("task").and_then(|v| v.as_object_mut()) {
                    task.remove("task_title");
                }
                if let Some(recovery) = value.get_mut("recovery").and_then(|v| v.as_object_mut()) {
                    recovery.remove("actions");
                }
                value.to_string()
            })
            .unwrap_or(bounded);
        let take = |value: &str| value.chars().take(256).collect::<String>();
        sha(&format!(
            "type={};scope_type={};scope_id={};status={};details_digest={};recommended_action={}",
            take(&kind),
            take(&scope_type),
            take(&scope_id),
            take(&status),
            sha(&canonical),
            take(&action),
        ))
    }
    async fn count(db: &SqliteDb, sql: &str, bind: &str) -> i64 {
        sqlx::query_scalar(sql)
            .bind(bind)
            .fetch_one(db.pool())
            .await
            .unwrap()
    }
    const INCIDENTS: &str = "SELECT count(*) FROM attention_projection WHERE attention_type='execution_failed' AND scope_id=?";
    const WAKES: &str =
        "SELECT count(*) FROM domain_event WHERE event_type='agent.wake.admitted' AND scope_id=?";
    const BLOCKERS: &str = "SELECT count(*) FROM agent_wake_blocker WHERE attention_id=?";

    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "Deployed incident").await;
    let task = project_task(&db, &project_id, "Unchanged blocker").await;
    let reason = "owner must repair the workspace";

    // The Task as the deployed build blocked it. Its condition is what the
    // upgrade migration restates from those fields.
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    sqlx::query("UPDATE task SET blocked_json=? WHERE id=?")
        .bind(
            json!({"kind":"workspace_error","reason":reason,"created_at":now_rfc3339()})
                .to_string(),
        )
        .bind(&task.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    db.sync_condition_in_tx(&mut tx, &task.id).await.unwrap();
    tx.commit().await.unwrap();

    // The event as the deployed build wrote it.
    let deployed_interruption = json!({"source":"blocked","kind":"workspace_error","reason":reason,"execution_id":"historical-reporter"});
    append_attention_event(
        &db,
        "task.interruption_changed",
        &task.id,
        &project_id,
        json!({
            "task_id": task.id,
            "task_version": task.version,
            "task_status": task.status,
            "requires_intervention": true,
            "interruption": deployed_interruption,
        }),
    )
    .await;
    let (event_id, event_sequence): (String, i64) = sqlx::query_as(
        "SELECT id,sequence FROM domain_event WHERE event_type='task.interruption_changed' AND entity_id=?",
    )
    .bind(&task.id)
    .fetch_one(db.pool())
    .await
    .unwrap();

    // The wake rows, written by code that is unchanged since the deployed
    // build. The incident row this pass also writes is replaced below.
    AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    )
    .project_once(100)
    .await
    .unwrap();
    let incident_id: String = sqlx::query_scalar(
        "SELECT id FROM attention_projection WHERE attention_type='execution_failed' AND scope_id=?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    let written: Value = serde_json::from_str(
        &sqlx::query_scalar::<_, String>(
            "SELECT details_json FROM attention_projection WHERE id=?",
        )
        .bind(&incident_id)
        .fetch_one(db.pool())
        .await
        .unwrap(),
    )
    .unwrap();

    // The incident row the deployed build stored for that event. The offers
    // are illustrative, not captured from a deployed database: both builds
    // remove `recovery.actions` before taking the digest.
    let deployed_actions = json!([{
        "action": {"verb": "retry", "reason": null, "fresh_session": null, "refresh_workspace": null, "reset_budget": null, "guidance": null},
        "accepts": ["guidance"],
        "allowed_actors": ["owner", "project_agent"],
        "reason": "blocked",
        "label": "Retry Task",
    }]);
    let deployed_details = json!({
        "source_event_id": event_id,
        "source_event_type": "task.interruption_changed",
        "source_sequence": event_sequence,
        "entity_type": "task",
        "entity_id": task.id,
        "scope_type": written["scope_type"],
        "scope_id": written["scope_id"],
        "task": {
            "task_title": "Unchanged blocker",
            "task_status": task.status,
            "task_version": task.version,
        },
        "failure_class": null,
        "retry_decision": null,
        "retry_action": null,
        "decision": null,
        "role": null,
        "stop_reason": null,
        "error": null,
        "interruption": deployed_interruption,
        "recovery": {
            "requires_intervention": true,
            "actions": deployed_actions,
            "automatic_retry": false,
        },
    });
    // Guard on the hard-coded row: this build writes the same document
    // except for the two members the builds are known to write differently
    // (the interruption it strips, and the offers it computes).
    let mut comparable = written.clone();
    comparable["interruption"] = deployed_interruption.clone();
    comparable["recovery"]["actions"] = deployed_actions.clone();
    comparable["task"]["task_version"] = json!(task.version);
    assert_eq!(
        comparable, deployed_details,
        "the hard-coded deployed incident row"
    );
    let deployed_details = deployed_details.to_string();
    sqlx::query("UPDATE attention_projection SET details_json=? WHERE id=?")
        .bind(&deployed_details)
        .bind(&incident_id)
        .execute(db.pool())
        .await
        .unwrap();

    // The digest the wake rows store is the deployed build's digest of that row.
    let digest = deployed_digest(&db, &incident_id).await;
    let latest: Option<String> = sqlx::query_scalar(
        "SELECT incident_digest FROM agent_wake_attention_latest WHERE attention_id=?",
    )
    .bind(&incident_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        latest.as_deref(),
        Some(digest.as_str()),
        "stored wake digest"
    );
    let stored_blockers: Vec<String> =
        sqlx::query_scalar("SELECT incident_digest FROM agent_wake_blocker WHERE attention_id=?")
            .bind(&incident_id)
            .fetch_all(db.pool())
            .await
            .unwrap();
    assert_eq!(
        stored_blockers,
        vec![digest.clone()],
        "stored blocker digest"
    );
    let wakes = count(&db, WAKES, &project_id).await;
    assert_eq!(
        wakes, 1,
        "the deployed build admitted one wake for this blocker"
    );

    // This build starts: its first projection pass and wake sweep.
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    service.project_once(100).await.unwrap();
    service.sweep_once_at(&now_rfc3339()).await.unwrap();
    assert_eq!(
        count(&db, INCIDENTS, &project_id).await,
        1,
        "no new incident"
    );
    assert_eq!(count(&db, WAKES, &project_id).await, wakes, "no wake");
    assert_eq!(count(&db, BLOCKERS, &incident_id).await, 1);
    assert_eq!(
        deployed_digest(&db, &incident_id).await,
        digest,
        "same digest"
    );
    let stored: String =
        sqlx::query_scalar("SELECT details_json FROM attention_projection WHERE id=?")
            .bind(&incident_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(
        stored, deployed_details,
        "the first pass leaves the stored incident alone"
    );

    // The first event this build writes about the same, unchanged blocker.
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    DomainEventRepo::append_event(&*db, CreateDomainEvent::task_interruption_changed(&current))
        .await
        .unwrap();
    service.project_once(100).await.unwrap();
    service.sweep_once_at(&now_rfc3339()).await.unwrap();
    assert_eq!(
        count(&db, INCIDENTS, &project_id).await,
        1,
        "no new incident"
    );
    assert_eq!(count(&db, WAKES, &project_id).await, wakes, "no wake");
    assert_eq!(count(&db, BLOCKERS, &incident_id).await, 1);
    assert_eq!(
        deployed_digest(&db, &incident_id).await,
        digest,
        "same digest"
    );

    // A real blocker change, through the real writer: one new event, the one
    // incident restated with a new digest, and never more than one wake.
    // (In this fixture the first wake's turn is still open and no second
    // wake is admitted, also when swept past the cooldown.)
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
            blocked_json: Some(Some(
                json!({"kind":"workspace_error","reason":"a different workspace blocker","created_at":now_rfc3339()})
                    .to_string(),
            )),
            failed_json: Some(None),
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    service.project_once(100).await.unwrap();
    service.sweep_once_at(&now_rfc3339()).await.unwrap();
    assert_eq!(
        count(&db, INCIDENTS, &project_id).await,
        1,
        "one incident, restated"
    );
    let changed = deployed_digest(&db, &incident_id).await;
    assert_ne!(changed, digest, "a real change is a new blocker");
    let events = count(
        &db,
        "SELECT count(*) FROM domain_event WHERE event_type='task.interruption_changed' AND entity_id=?",
        &task.id,
    )
    .await;
    assert_eq!(
        events, 3,
        "deployed event, unchanged restatement, one real change"
    );
    // The admitted wake holds a cooldown; sweep past it, twice.
    for minutes in [10, 20] {
        service
            .sweep_once_at(&(Utc::now() + Duration::minutes(minutes)).to_rfc3339())
            .await
            .unwrap();
    }
    let new_wakes = count(&db, WAKES, &project_id).await - wakes;
    let new_blockers = count(&db, BLOCKERS, &incident_id).await - 1;
    println!("after a real change: wakes +{new_wakes}, blockers +{new_blockers}");
    assert!(
        new_wakes <= 1 && new_blockers <= 1,
        "at most one wake for one change"
    );
}

// Exercise the seven event consumers with fresh runtime objects, including the
// startup Attention tick, then flush the ignored tail before stopping.
async fn restart_wake_cycle(db: &Arc<SqliteDb>, service: Arc<AttentionService>) {
    use services::worker_runtime::{conflict_hotspot::ConflictHotspotConsumer, WorkerRuntime};
    let bus = Arc::new(events::EventBus::new(32));
    let notifications = Arc::new(services::NotificationService::new(db.clone(), bus.clone()));
    let tasks = Arc::new(services::TaskService::new_for_test(db.clone(), bus.clone()));
    let attention = WorkerRuntime::new(db.clone(), service);
    let wake = WorkerRuntime::new(
        db.clone(),
        Arc::new(services::WakeTurnConsumer::new(db.clone())),
    );
    let coordination = WorkerRuntime::new(
        db.clone(),
        Arc::new(services::CoordinationOutcomeConsumer::new(db.clone())),
    );
    let memory = WorkerRuntime::new(
        db.clone(),
        Arc::new(services::AgentChatMemoryConsumer::new(db.clone())),
    );
    let hooks = WorkerRuntime::new(
        db.clone(),
        Arc::new(services::ProjectHookService::new(
            db.clone(),
            bus,
            tasks,
            notifications.clone(),
        )),
    );
    let notifications = WorkerRuntime::new(db.clone(), notifications);
    let conflicts = WorkerRuntime::new(
        db.clone(),
        Arc::new(ConflictHotspotConsumer::new(db.clone())),
    );
    for _ in 0..3 {
        attention.run_once(100).await.unwrap();
        wake.run_once(100).await.unwrap();
        coordination.run_once(100).await.unwrap();
        memory.run_once(100).await.unwrap();
        hooks.run_once(100).await.unwrap();
        notifications.run_once(100).await.unwrap();
        conflicts.run_once(100).await.unwrap();
        tokio::time::pause();
        tokio::time::advance(std::time::Duration::from_secs(6)).await;
        tokio::time::resume();
    }
}

const RESTART_WAKE_TABLES: &[&str] = &[
    "domain_event",
    "agent_wake_disposition",
    "agent_wake_disposition_current",
    "event_consumer_cursor",
    "agent_wake_attention_latest",
];

async fn restart_wake_snapshot(db: &SqliteDb) -> Vec<String> {
    use sqlx::Row;
    let mut result = Vec::new();
    for table in RESTART_WAKE_TABLES {
        let columns = sqlx::query(&format!("PRAGMA table_info({table})"))
            .fetch_all(db.pool())
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get::<String, _>("name"))
            .collect::<Vec<_>>();
        let fields = columns
            .iter()
            .map(|c| format!("'{c}',\"{c}\""))
            .collect::<Vec<_>>()
            .join(",");
        result.push(
            sqlx::query_scalar(&format!(
            "SELECT json_group_array(json_object({fields})) FROM (SELECT * FROM {table} ORDER BY 1)"
        ))
            .fetch_one(db.pool())
            .await
            .unwrap(),
        );
    }
    result
}

async fn restart_wake_observe_writes(db: &SqliteDb) {
    sqlx::query("CREATE TABLE restart_wake_writes (table_name TEXT, operation TEXT)")
        .execute(db.pool())
        .await
        .unwrap();
    for table in RESTART_WAKE_TABLES {
        for operation in ["INSERT", "UPDATE", "DELETE"] {
            sqlx::raw_sql(&format!(
                "CREATE TRIGGER observe_{table}_{operation} AFTER {operation} ON {table} BEGIN INSERT INTO restart_wake_writes VALUES ('{table}', '{operation}'); END;"
            )).execute(db.pool()).await.unwrap();
        }
    }
}

#[tokio::test]
async fn restart_wake_imported_merges_are_write_free_and_a_real_change_wakes_once() {
    let db = database().await;
    let project_id = "restart-merge-project".to_owned();
    let at = "2026-01-01T00:00:00Z";
    let owner = "restart-owner";
    db::UserRepo::create_user(
        &*db,
        &db::User {
            id: owner.into(),
            email: "restart@example.test".into(),
            password_hash: "test".into(),
            display_name: None,
            is_admin: false,
            created_at: at.into(),
            updated_at: at.into(),
        },
    )
    .await
    .unwrap();
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.clone(),
            owner_id: Some(owner.into()),
            name: "Imported merges".into(),
            primary_repo_id: None,
            settings: "{}".into(),
            workflow_definition: "{}".into(),
            created_at: at.into(),
            updated_at: at.into(),
        },
    )
    .await
    .unwrap();
    let mut task_ids = Vec::new();
    for (index, title) in ["merge_held", "merge_paused"].into_iter().enumerate() {
        let task = project_task(&db, &project_id, title).await;
        let attempt = format!("imported-{title}");
        sqlx::query("INSERT INTO integration_attempt (id,task_id,task_ref,project_ref,queue_seq,current,admission_key,expected_status,expected_epoch,observed_task_version,enqueued_at,state,failure_kind,failure_message,import_source_json,created_at,updated_at) VALUES (?,?,?,?,1,0,?,'merging',0,1,?,'quarantined','owner_required','missing execution or pinned workspace','{}',?,?)")
            .bind(&attempt).bind(&task.id).bind(&task.id).bind(&project_id).bind(&attempt)
            .bind(at).bind(at).bind(at).execute(db.pool()).await.unwrap();
        let interruption = serde_json::json!({"source":"integration", "cause":"owner_required", "kind":"review_needs_owner", "owner_id":null, "reason":"missing execution or pinned workspace"});
        let reason = serde_json::json!({"kind":"deferred", "attempt_id":attempt, "cause":"owner_required", "owner_id":null, "message":"missing execution or pinned workspace"});
        let mut evidence =
            serde_json::to_value(db::task_condition::ConditionEvidence::default()).unwrap();
        evidence["material"] =
            serde_json::json!({"requires_intervention":true,"interruption":interruption});
        evidence["witnesses"] = serde_json::json!([{"kind":"integration", "reason":reason}]);
        let condition = serde_json::json!({"kind":"parked", "primary":{"kind":"integration", "reason":reason}, "additional":[], "resume":{"kind":"integration", "attempt_id":attempt}, "since":null, "evidence":evidence});
        db::task_condition::decode(&condition.to_string()).unwrap();
        sqlx::query("UPDATE task SET status='merging', condition_json=? WHERE id=?")
            .bind(condition.to_string())
            .bind(&task.id)
            .execute(db.pool())
            .await
            .unwrap();
        append_attention_event_at(
            &db,
            "task.interruption_changed",
            &task.id,
            &project_id,
            serde_json::json!({"requires_intervention":true, "interruption":interruption}),
            &format!("2026-01-01T00:00:0{index}Z"),
        )
        .await;
        task_ids.push(task.id);
    }
    let new_service = || {
        Arc::new(AttentionService::new(
            db.clone(),
            Arc::new(services::WakeTurnConsumer::new(db.clone())),
        ))
    };
    restart_wake_cycle(&db, new_service()).await;
    let decisions: Vec<(String, String)> = sqlx::query_as("SELECT attention_id,incident_digest FROM agent_wake_disposition ORDER BY source_event_sequence")
        .fetch_all(db.pool()).await.unwrap();
    assert_eq!(decisions.len(), 2);
    for (id, digest) in &decisions {
        assert_eq!(
            *digest,
            services::wake_attention_incident_digest(&db.get_attention(id).await.unwrap().unwrap())
        );
    }
    let cursors: Vec<i64> = sqlx::query_scalar("SELECT last_sequence FROM event_consumer_cursor")
        .fetch_all(db.pool())
        .await
        .unwrap();
    let head: i64 = sqlx::query_scalar("SELECT MAX(sequence) FROM domain_event")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(cursors, vec![head; 7]);
    let before = restart_wake_snapshot(&db).await;
    restart_wake_observe_writes(&db).await;
    // Reconstruct twice: neither a second nor a third start may write anything.
    for _ in 0..2 {
        restart_wake_cycle(&db, new_service()).await;
        let events: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM domain_event WHERE event_type='agent.wake.setup_required'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(
            events, 2,
            "an unchanged startup must not add a batch-shaped setup decision"
        );
        assert_eq!(
            before,
            restart_wake_snapshot(&db).await,
            "all columns, including timestamps and cursor versions"
        );
        let writes: Vec<(String, String)> = sqlx::query_as("SELECT * FROM restart_wake_writes")
            .fetch_all(db.pool())
            .await
            .unwrap();
        assert!(
            writes.is_empty(),
            "even same-value writes are forbidden: {writes:?}"
        );
    }
    // Real setup changes after restart admit the outstanding blocker batch once.
    let responder = "restart-responder";
    identity(&db, responder).await;
    sqlx::query("UPDATE agent_identity SET owner_id=? WHERE id=?")
        .bind(owner)
        .bind(responder)
        .execute(db.pool())
        .await
        .unwrap();
    let version: i64 = sqlx::query_scalar("SELECT version FROM project_agent_binding WHERE project_id=? AND state='agent_setup_required'")
        .bind(&project_id).fetch_one(db.pool()).await.unwrap();
    services::AgentChatService::new(db.clone())
        .set_project_binding(services::SetProjectAgentBindingInput {
            actor_user_id: owner.into(),
            project_id: project_id.clone(),
            identity_id: Some(responder.into()),
            state: "active".into(),
            autonomy_policy_json: "{}".into(),
            permission_ceiling_json: "{}".into(),
            subscriptions_json: "[]".into(),
            wake_budget: 10,
            expected_version: Some(version),
            replacement_reason: None,
        })
        .await
        .unwrap();
    restart_wake_cycle(&db, new_service()).await;
    restart_wake_cycle(&db, new_service()).await;
    let admitted: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type='agent.wake.admitted'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    let turns: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_turn_job")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(
        (admitted, turns),
        (1, 1),
        "a real setup change wakes the two blockers exactly once"
    );
    // A completed turn consumes the old digest. Resolve the other batch member
    // so a material change to this Task must be the source of the next wake.
    sqlx::query("UPDATE agent_chat_turn_job SET status='succeeded',version=version+1")
        .execute(db.pool())
        .await
        .unwrap();
    let other: String = sqlx::query_scalar(
        "SELECT id FROM attention_projection WHERE json_extract(details_json,'$.entity_id')=?",
    )
    .bind(&task_ids[1])
    .fetch_one(db.pool())
    .await
    .unwrap();
    let other = db.get_attention(&other).await.unwrap().unwrap();
    db.update_attention_lifecycle(UpdateAttentionLifecycle {
        id: other.id,
        expected_version: other.version,
        status: "resolved".into(),
        acknowledged_at: None,
        snoozed_until: None,
        resolved_at: Some(Some(now_rfc3339())),
        updated_by_user_id: None,
        updated_at: now_rfc3339(),
    })
    .await
    .unwrap();
    let task = TaskRepo::get_by_id(&*db, &task_ids[0], false)
        .await
        .unwrap()
        .unwrap();
    let mut condition = serde_json::to_value(&task.condition).unwrap();
    let message = "the imported merge now requires a different owner repair";
    condition["primary"]["reason"]["message"] = serde_json::json!(message);
    condition["evidence"]["witnesses"][0]["reason"]["message"] = serde_json::json!(message);
    condition["evidence"]["material"]["interruption"]["reason"] = serde_json::json!(message);
    db::task_condition::decode(&condition.to_string()).unwrap();
    sqlx::query("UPDATE task SET condition_json=?,version=version+1 WHERE id=?")
        .bind(condition.to_string())
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
    let changed = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    db.append_event(CreateDomainEvent::task_interruption_changed(&changed))
        .await
        .unwrap();
    let service = new_service();
    // Project first, then advance admission time beyond the existing cooldown.
    services::worker_runtime::WorkerRuntime::new(db.clone(), service.clone())
        .run_once(100)
        .await
        .unwrap();
    assert_eq!(
        service.sweep_once_at("2030-01-01T00:00:00Z").await.unwrap(),
        1
    );
    restart_wake_cycle(&db, new_service()).await;
    restart_wake_cycle(&db, new_service()).await;
    let admitted: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type='agent.wake.admitted'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    let turns: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_turn_job")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(
        (admitted, turns),
        (2, 2),
        "a material blocker change after restart adds exactly one wake and turn"
    );
}

#[tokio::test]
async fn restart_wake_latest_upsert_changes_only_material_values() {
    let db = database().await;
    append_attention_event(
        &db,
        "validation.failed",
        "latest-task",
        "latest-project",
        serde_json::json!({}),
    )
    .await;
    let service = AttentionService::new(
        db.clone(),
        Arc::new(services::WakeTurnConsumer::new(db.clone())),
    );
    service.project_once(100).await.unwrap();
    let id: String = sqlx::query_scalar("SELECT id FROM attention_projection")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    // Equal NULL identities must be compared as equal. Timestamps are not material.
    for (index, (digest, disposition, reason, identity, expected)) in [
        ("digest", "setup_required", "missing", None, 1),
        ("digest", "setup_required", "missing", None, 0),
        ("changed", "setup_required", "missing", None, 1),
        ("changed", "deferred", "missing", None, 1),
        ("changed", "deferred", "cooldown", None, 1),
        ("changed", "deferred", "cooldown", Some("identity"), 1),
        ("changed", "deferred", "cooldown", Some("identity"), 0),
        ("changed", "deferred", "cooldown", None, 1),
    ]
    .into_iter()
    .enumerate()
    {
        let timestamp = format!("2030-01-01T00:00:0{index}Z");
        let before: String = sqlx::query_scalar(
            "SELECT updated_at FROM agent_wake_attention_latest WHERE attention_id=?",
        )
        .bind(&id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        assert_eq!(
            db.record_latest_attention_wake_in_tx(
                &mut tx,
                db::RecordLatestAttentionWakeInTxInput {
                    attention_id: &id,
                    existing_attention_id: &id,
                    incident_digest: digest,
                    disposition,
                    reason,
                    identity_id: identity,
                    updated_at: &timestamp,
                }
            )
            .await
            .unwrap(),
            expected
        );
        let after: String = sqlx::query_scalar(
            "SELECT updated_at FROM agent_wake_attention_latest WHERE attention_id=?",
        )
        .bind(&id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        assert_eq!(after, if expected == 0 { before } else { timestamp });
    }
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn restart_wake_checkpoint_failure_rolls_back_decision_and_disposition() {
    use services::worker_runtime::WorkerRuntime;
    let db = database().await;
    let service = Arc::new(AttentionService::new(
        db.clone(),
        Arc::new(services::WakeTurnConsumer::new(db.clone())),
    ));
    let runtime = WorkerRuntime::new(db.clone(), service);
    runtime.run_once(100).await.unwrap(); // initialize the checkpoint before fault injection
    append_attention_event(
        &db,
        "validation.failed",
        "atomic-task",
        "atomic-project",
        serde_json::json!({}),
    )
    .await;
    let before = restart_wake_snapshot(&db).await;
    sqlx::raw_sql("CREATE TRIGGER fail_wake_checkpoint BEFORE UPDATE ON event_consumer_cursor WHEN NEW.consumer_name='attention_projection' BEGIN SELECT RAISE(ABORT, 'checkpoint fault'); END;")
        .execute(db.pool()).await.unwrap();
    assert!(runtime.run_once(100).await.is_err());
    assert_eq!(
        before,
        restart_wake_snapshot(&db).await,
        "the audit event, both disposition rows and latest decision roll back with the cursor"
    );
    let incidents: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM attention_projection")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(incidents, 0);
    sqlx::query("DROP TRIGGER fail_wake_checkpoint")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(runtime.run_once(100).await.unwrap(), 1);
    let counts: (i64, i64, i64, i64) = sqlx::query_as("SELECT (SELECT COUNT(*) FROM domain_event WHERE event_type='agent.wake.setup_required'), (SELECT COUNT(*) FROM agent_wake_disposition), (SELECT COUNT(*) FROM agent_wake_disposition_current), (SELECT COUNT(*) FROM agent_wake_attention_latest)")
        .fetch_one(db.pool()).await.unwrap();
    assert_eq!(counts, (1, 1, 1, 1));
    let source: i64 = sqlx::query_scalar(
        "SELECT sequence FROM domain_event WHERE event_type='validation.failed'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    let cursor = db
        .get_consumer_cursor("attention_projection")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor.last_sequence, source);
}

// Two imported merge blockers in a Project without a Project Agent: the state
// the upgrade rehearsal restarted. Returns the Project, its owner and Tasks.
async fn restart_wake_two_merge_blockers(db: &Arc<SqliteDb>) -> (String, String, Vec<String>) {
    let project_id = "restart-review-project".to_owned();
    let at = "2026-01-01T00:00:00Z";
    let owner = "restart-review-owner".to_owned();
    db::UserRepo::create_user(
        &**db,
        &db::User {
            id: owner.clone(),
            email: "restart-review@example.test".into(),
            password_hash: "test".into(),
            display_name: None,
            is_admin: false,
            created_at: at.into(),
            updated_at: at.into(),
        },
    )
    .await
    .unwrap();
    ProjectRepo::create(
        &**db,
        CreateProject {
            id: project_id.clone(),
            owner_id: Some(owner.clone()),
            name: "Imported merges".into(),
            primary_repo_id: None,
            settings: "{}".into(),
            workflow_definition: "{}".into(),
            created_at: at.into(),
            updated_at: at.into(),
        },
    )
    .await
    .unwrap();
    let mut task_ids = Vec::new();
    for (index, title) in ["merge_held", "merge_paused"].into_iter().enumerate() {
        let task = project_task(db, &project_id, title).await;
        let attempt = format!("imported-{title}");
        sqlx::query("INSERT INTO integration_attempt (id,task_id,task_ref,project_ref,queue_seq,current,admission_key,expected_status,expected_epoch,observed_task_version,enqueued_at,state,failure_kind,failure_message,import_source_json,created_at,updated_at) VALUES (?,?,?,?,1,0,?,'merging',0,1,?,'quarantined','owner_required','missing execution or pinned workspace','{}',?,?)")
            .bind(&attempt).bind(&task.id).bind(&task.id).bind(&project_id).bind(&attempt)
            .bind(at).bind(at).bind(at).execute(db.pool()).await.unwrap();
        let interruption = serde_json::json!({"source":"integration", "cause":"owner_required", "kind":"review_needs_owner", "owner_id":null, "reason":"missing execution or pinned workspace"});
        let reason = serde_json::json!({"kind":"deferred", "attempt_id":attempt, "cause":"owner_required", "owner_id":null, "message":"missing execution or pinned workspace"});
        let mut evidence =
            serde_json::to_value(db::task_condition::ConditionEvidence::default()).unwrap();
        evidence["material"] =
            serde_json::json!({"requires_intervention":true,"interruption":interruption});
        evidence["witnesses"] = serde_json::json!([{"kind":"integration", "reason":reason}]);
        let condition = serde_json::json!({"kind":"parked", "primary":{"kind":"integration", "reason":reason}, "additional":[], "resume":{"kind":"integration", "attempt_id":attempt}, "since":null, "evidence":evidence});
        db::task_condition::decode(&condition.to_string()).unwrap();
        sqlx::query("UPDATE task SET status='merging', condition_json=? WHERE id=?")
            .bind(condition.to_string())
            .bind(&task.id)
            .execute(db.pool())
            .await
            .unwrap();
        append_attention_event_at(
            db,
            "task.interruption_changed",
            &task.id,
            &project_id,
            serde_json::json!({"requires_intervention":true, "interruption":interruption}),
            &format!("2026-01-01T00:00:0{index}Z"),
        )
        .await;
        task_ids.push(task.id);
    }
    (project_id, owner, task_ids)
}

fn restart_wake_service(db: &Arc<SqliteDb>) -> Arc<AttentionService> {
    Arc::new(AttentionService::new(
        db.clone(),
        Arc::new(services::WakeTurnConsumer::new(db.clone())),
    ))
}

async fn restart_wake_bind(db: &Arc<SqliteDb>, project_id: &str, owner: &str, responder: &str) {
    identity(db, responder).await;
    sqlx::query("UPDATE agent_identity SET owner_id=? WHERE id=?")
        .bind(owner)
        .bind(responder)
        .execute(db.pool())
        .await
        .unwrap();
    let version: i64 = sqlx::query_scalar(
        "SELECT version FROM project_agent_binding WHERE project_id=? AND state IN ('agent_setup_required','active')",
    )
    .bind(project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    services::AgentChatService::new(db.clone())
        .set_project_binding(services::SetProjectAgentBindingInput {
            actor_user_id: owner.into(),
            project_id: project_id.into(),
            identity_id: Some(responder.into()),
            state: "active".into(),
            autonomy_policy_json: "{}".into(),
            permission_ceiling_json: "{}".into(),
            subscriptions_json: "[]".into(),
            wake_budget: 10,
            expected_version: Some(version),
            replacement_reason: None,
        })
        .await
        .unwrap();
}

async fn restart_wake_count(db: &SqliteDb, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(db.pool()).await.unwrap()
}

/// A responder that is bound but cannot run (its Agent is paused) is a real
/// setup change: each open blocker records one new setup decision naming the
/// bound Agent, a restart records nothing more, and unpausing wakes once.
#[tokio::test]
async fn restart_wake_bound_but_paused_responder_records_one_setup_decision_per_blocker() {
    let db = database().await;
    let (project_id, owner, _) = restart_wake_two_merge_blockers(&db).await;
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    let setup = "SELECT COUNT(*) FROM domain_event WHERE event_type='agent.wake.setup_required'";
    assert_eq!(restart_wake_count(&db, setup).await, 2);

    let responder = "restart-paused-responder";
    restart_wake_bind(&db, &project_id, &owner, responder).await;
    sqlx::query("UPDATE agent_identity SET paused=1 WHERE id=?")
        .bind(responder)
        .execute(db.pool())
        .await
        .unwrap();
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    assert_eq!(
        restart_wake_count(&db, setup).await,
        4,
        "the changed responder setup records one decision per open blocker"
    );
    assert_eq!(
        restart_wake_count(&db, "SELECT COUNT(*) FROM agent_wake_disposition").await,
        4
    );
    let latest: Vec<Option<String>> =
        sqlx::query_scalar("SELECT identity_id FROM agent_wake_attention_latest ORDER BY 1")
            .fetch_all(db.pool())
            .await
            .unwrap();
    assert_eq!(latest, vec![Some(responder.to_owned()); 2]);

    let before = restart_wake_snapshot(&db).await;
    restart_wake_observe_writes(&db).await;
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    assert_eq!(before, restart_wake_snapshot(&db).await);
    let writes: i64 = restart_wake_count(&db, "SELECT COUNT(*) FROM restart_wake_writes").await;
    assert_eq!(writes, 0, "an unchanged restart writes nothing");

    sqlx::query("UPDATE agent_identity SET paused=0 WHERE id=?")
        .bind(responder)
        .execute(db.pool())
        .await
        .unwrap();
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    assert_eq!(
        restart_wake_count(
            &db,
            "SELECT COUNT(*) FROM domain_event WHERE event_type='agent.wake.admitted'"
        )
        .await,
        1,
        "the runnable responder is woken exactly once for both blockers"
    );
}

/// The deployed release's startup sweep stored one extra setup decision under
/// a batch digest. After the upgrade that row is inert: the first start writes
/// nothing, and the next real change still wakes exactly once.
#[tokio::test]
async fn restart_wake_stored_batch_digest_decision_is_inert_after_upgrade() {
    use sha2::{Digest, Sha256};
    let db = database().await;
    let (project_id, owner, _) = restart_wake_two_merge_blockers(&db).await;
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    // Seed what the deployed sweep added: a copy of the first member's setup
    // decision keyed and digested by the batch (both member digests).
    let members: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT e.id, d.incident_digest, e.dedupe_key FROM domain_event e JOIN agent_wake_disposition d ON d.source_event_id=e.id ORDER BY e.sequence",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(members.len(), 2);
    let mut hasher = Sha256::new();
    hasher.update(format!("{}:{}", members[0].1, members[1].1).as_bytes());
    hasher.update([0]);
    let batch = format!("sha256:{}", hex::encode(hasher.finalize()));
    let event_id = new_uuid_v4();
    sqlx::query("INSERT INTO domain_event (id,event_type,entity_type,entity_id,actor_type,actor_id,scope_type,scope_id,correlation_id,causation_id,causation_depth,dedupe_key,payload_json,created_at) SELECT ?,event_type,entity_type,entity_id,actor_type,actor_id,scope_type,scope_id,correlation_id,causation_id,causation_depth,?,json_set(payload_json,'$.incident_digest',?),'2026-01-02T00:00:00Z' FROM domain_event WHERE id=?")
        .bind(&event_id).bind(format!("agent-wake-decision:agent-wake-setup_required:{batch}")).bind(&batch).bind(&members[0].0)
        .execute(db.pool()).await.unwrap();
    let disposition_id = new_uuid_v4();
    sqlx::query("INSERT INTO agent_wake_disposition (id,consumer_name,source_event_id,source_event_sequence,attempt_number,max_attempts,disposition,reason,turn_job_id,attention_id,retry_at,incident_key,incident_digest,binding_id,binding_version,profile_id,profile_version,provenance_json,parent_disposition_id,created_at,updated_at) SELECT ?,consumer_name,?,(SELECT sequence FROM domain_event WHERE id=?),attempt_number,max_attempts,disposition,reason,turn_job_id,attention_id,retry_at,incident_key,?,binding_id,binding_version,profile_id,profile_version,provenance_json,parent_disposition_id,created_at,updated_at FROM agent_wake_disposition WHERE source_event_id=?")
        .bind(&disposition_id).bind(&event_id).bind(&event_id).bind(&batch).bind(&members[0].0)
        .execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO agent_wake_disposition_current (consumer_name,source_event_id,disposition_id,attempt_number,updated_at) SELECT consumer_name,source_event_id,id,attempt_number,updated_at FROM agent_wake_disposition WHERE id=?")
        .bind(&disposition_id).execute(db.pool()).await.unwrap();
    // The deployed consumers had already read past the stored decision.
    sqlx::query(
        "UPDATE event_consumer_cursor SET last_sequence=(SELECT MAX(sequence) FROM domain_event)",
    )
    .execute(db.pool())
    .await
    .unwrap();

    let before = restart_wake_snapshot(&db).await;
    restart_wake_observe_writes(&db).await;
    for _ in 0..2 {
        restart_wake_cycle(&db, restart_wake_service(&db)).await;
        assert_eq!(before, restart_wake_snapshot(&db).await);
        let writes: i64 = restart_wake_count(&db, "SELECT COUNT(*) FROM restart_wake_writes").await;
        assert_eq!(
            writes, 0,
            "the first start after the upgrade writes nothing"
        );
    }

    restart_wake_bind(&db, &project_id, &owner, "restart-upgrade-responder").await;
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    assert_eq!(
        restart_wake_count(
            &db,
            "SELECT COUNT(*) FROM domain_event WHERE event_type='agent.wake.admitted'"
        )
        .await,
        1
    );
}

/// With a bound responder that cannot run, the deployed release stored only
/// the batch decision (projection deferred the blockers to the sweep). The
/// first start after the upgrade records each blocker's own decision once;
/// later starts write nothing.
#[tokio::test]
async fn restart_wake_unready_responder_batch_decision_is_replaced_once_after_upgrade() {
    use sha2::{Digest, Sha256};
    let db = database().await;
    // Bind a paused responder before the blockers are projected.
    let (project_id, owner, _) = restart_wake_two_merge_blockers(&db).await;
    let responder = "restart-unready-responder";
    restart_wake_bind(&db, &project_id, &owner, responder).await;
    sqlx::query("UPDATE agent_identity SET paused=1 WHERE id=?")
        .bind(responder)
        .execute(db.pool())
        .await
        .unwrap();
    // Projection defers bound blockers; the next start's sweep decides them.
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    let members: Vec<(String, String)> = sqlx::query_as(
        "SELECT e.id, d.incident_digest FROM domain_event e JOIN agent_wake_disposition d ON d.source_event_id=e.id WHERE e.event_type='agent.wake.setup_required' ORDER BY e.sequence",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    let decided: Vec<(String, String)> = sqlx::query_as(
        "SELECT event_type, payload_json FROM domain_event WHERE event_type LIKE 'agent.wake.%' ORDER BY sequence",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(
        members.len(),
        2,
        "only the sweep decided these blockers: {decided:?}"
    );
    // Replace the per-blocker decisions with the deployed batch decision.
    let mut hasher = Sha256::new();
    hasher.update(format!("{}:{}", members[0].1, members[1].1).as_bytes());
    hasher.update([0]);
    let batch = format!("sha256:{}", hex::encode(hasher.finalize()));
    let event_id = new_uuid_v4();
    sqlx::query("INSERT INTO domain_event (id,event_type,entity_type,entity_id,actor_type,actor_id,scope_type,scope_id,correlation_id,causation_id,causation_depth,dedupe_key,payload_json,created_at) SELECT ?,event_type,entity_type,entity_id,actor_type,actor_id,scope_type,scope_id,correlation_id,causation_id,causation_depth,?,json_set(payload_json,'$.incident_digest',?),created_at FROM domain_event WHERE id=?")
        .bind(&event_id).bind(format!("agent-wake-decision:agent-wake-setup_required:{batch}")).bind(&batch).bind(&members[0].0)
        .execute(db.pool()).await.unwrap();
    let disposition_id = new_uuid_v4();
    sqlx::query("INSERT INTO agent_wake_disposition (id,consumer_name,source_event_id,source_event_sequence,attempt_number,max_attempts,disposition,reason,turn_job_id,attention_id,retry_at,incident_key,incident_digest,binding_id,binding_version,profile_id,profile_version,provenance_json,parent_disposition_id,created_at,updated_at) SELECT ?,consumer_name,?,(SELECT sequence FROM domain_event WHERE id=?),attempt_number,max_attempts,disposition,reason,turn_job_id,attention_id,retry_at,incident_key,?,binding_id,binding_version,profile_id,profile_version,provenance_json,parent_disposition_id,created_at,updated_at FROM agent_wake_disposition WHERE source_event_id=?")
        .bind(&disposition_id).bind(&event_id).bind(&event_id).bind(&batch).bind(&members[0].0)
        .execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO agent_wake_disposition_current (consumer_name,source_event_id,disposition_id,attempt_number,updated_at) SELECT consumer_name,source_event_id,id,attempt_number,updated_at FROM agent_wake_disposition WHERE id=?")
        .bind(&disposition_id).execute(db.pool()).await.unwrap();
    for (id, _) in &members {
        for sql in [
            "DELETE FROM agent_wake_disposition_current WHERE source_event_id=?",
            "DELETE FROM agent_wake_disposition WHERE source_event_id=?",
            "DELETE FROM domain_event WHERE id=?",
        ] {
            sqlx::query(sql).bind(id).execute(db.pool()).await.unwrap();
        }
    }
    sqlx::query(
        "UPDATE event_consumer_cursor SET last_sequence=(SELECT MAX(sequence) FROM domain_event)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    restart_wake_observe_writes(&db).await;
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    let writes: Vec<(String, String)> = sqlx::query_as(
        "SELECT table_name, operation FROM restart_wake_writes WHERE table_name <> 'event_consumer_cursor' ORDER BY 1, 2",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    let expected = [
        ("agent_wake_disposition", "INSERT"),
        ("agent_wake_disposition", "INSERT"),
        ("agent_wake_disposition_current", "INSERT"),
        ("agent_wake_disposition_current", "INSERT"),
        ("domain_event", "INSERT"),
        ("domain_event", "INSERT"),
    ]
    .map(|(t, o)| (t.to_owned(), o.to_owned()));
    assert_eq!(writes, expected, "once: one decision per blocker");
    sqlx::query("DELETE FROM restart_wake_writes")
        .execute(db.pool())
        .await
        .unwrap();
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    let writes: i64 = restart_wake_count(&db, "SELECT COUNT(*) FROM restart_wake_writes").await;
    assert_eq!(writes, 0, "not forever");
}

/// A blocker whose Task went away while Forge was down is settled by the
/// startup sweep, even when it is not the first member of the Project's
/// blockers; the other blocker's decision is left untouched.
#[tokio::test]
async fn restart_wake_blocker_closed_during_downtime_is_settled_once() {
    let db = database().await;
    let (_, _, task_ids) = restart_wake_two_merge_blockers(&db).await;
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    sqlx::query("UPDATE task SET deleted_at=? WHERE id=?")
        .bind(now_rfc3339())
        .bind(&task_ids[1])
        .execute(db.pool())
        .await
        .unwrap();
    restart_wake_observe_writes(&db).await;
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    let statuses: Vec<(String, String)> = sqlx::query_as(
        "SELECT json_extract(details_json,'$.entity_id'), status FROM attention_projection ORDER BY occurred_at",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(
        statuses,
        vec![
            (task_ids[0].clone(), "open".to_owned()),
            (task_ids[1].clone(), "resolved".to_owned())
        ]
    );
    let suppressed: Vec<String> = sqlx::query_scalar(
        "SELECT json_extract(payload_json,'$.reason') FROM domain_event WHERE event_type='agent.wake.suppressed'",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(suppressed, vec!["resolved_incident".to_owned()]);
    let setup = "SELECT COUNT(*) FROM domain_event WHERE event_type='agent.wake.setup_required'";
    assert_eq!(restart_wake_count(&db, setup).await, 2);
    sqlx::query("DELETE FROM restart_wake_writes")
        .execute(db.pool())
        .await
        .unwrap();
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    let writes: i64 = restart_wake_count(&db, "SELECT COUNT(*) FROM restart_wake_writes").await;
    assert_eq!(writes, 0);
}

/// The latest-decision upsert compares every stored value except the
/// timestamp. A new column must be added to that comparison too.
#[tokio::test]
async fn restart_wake_latest_guard_covers_every_column() {
    use sqlx::Row;
    let db = database().await;
    let columns: Vec<String> = sqlx::query("PRAGMA table_info(agent_wake_attention_latest)")
        .fetch_all(db.pool())
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect();
    assert_eq!(
        columns,
        [
            "attention_id",
            "incident_digest",
            "disposition",
            "reason",
            "identity_id",
            "updated_at"
        ],
        "update the guard in record_latest_attention_wake_in_tx for a new column"
    );
}
