//! Shared fixtures and helpers for attention service tests.

use super::*;
use crate::test_services as services;

pub(super) async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    Arc::new(SqliteDb::new(pool))
}

pub(super) async fn identity(db: &SqliteDb, id: &str) {
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
pub(super) async fn configured_project(
    db: &Arc<SqliteDb>,
    identity_id: &str,
    name: &str,
) -> String {
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

pub(super) async fn project_task(db: &Arc<SqliteDb>, project_id: &str, title: &str) -> db::Task {
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

pub(super) async fn task_execution(
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

pub(super) async fn append_attention_event(
    db: &Arc<SqliteDb>,
    event_type: &str,
    task_id: &str,
    project_id: &str,
    payload: serde_json::Value,
) {
    append_attention_event_at(db, event_type, task_id, project_id, payload, &now_rfc3339()).await;
}

pub(super) async fn append_attention_event_at(
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

pub(super) fn request(identity_id: &str, incident_key: &str) -> WakeAdmissionRequest {
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

/// Seed a Project with `workflow_definition` and one Task parked in `review`,
/// then project its `task.transitioned` event. Returns the attention rows
/// materialized for the Project.
pub(super) async fn review_attention_rows(
    db: &Arc<SqliteDb>,
    workflow_definition: &str,
) -> Vec<String> {
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

// Exercise the seven event consumers with fresh runtime objects, including the
// startup Attention tick, then flush the ignored tail before stopping.
pub(super) async fn restart_wake_cycle(db: &Arc<SqliteDb>, service: Arc<AttentionService>) {
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

pub(super) const RESTART_WAKE_TABLES: &[&str] = &[
    "domain_event",
    "agent_wake_disposition",
    "agent_wake_disposition_current",
    "event_consumer_cursor",
    "agent_wake_attention_latest",
];

pub(super) async fn restart_wake_snapshot(db: &SqliteDb) -> Vec<String> {
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

pub(super) async fn restart_wake_observe_writes(db: &SqliteDb) {
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

// Two imported merge blockers in a Project without a Project Agent: the state
// the upgrade rehearsal restarted. Returns the Project, its owner and Tasks.
pub(super) async fn restart_wake_two_merge_blockers(
    db: &Arc<SqliteDb>,
) -> (String, String, Vec<String>) {
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

pub(super) fn restart_wake_service(db: &Arc<SqliteDb>) -> Arc<AttentionService> {
    Arc::new(AttentionService::new(
        db.clone(),
        Arc::new(services::WakeTurnConsumer::new(db.clone())),
    ))
}

pub(super) async fn restart_wake_bind(
    db: &Arc<SqliteDb>,
    project_id: &str,
    owner: &str,
    responder: &str,
) {
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

pub(super) async fn restart_wake_count(db: &SqliteDb, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(db.pool()).await.unwrap()
}
