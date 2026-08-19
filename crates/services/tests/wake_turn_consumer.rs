use std::sync::Arc;

use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentRepo, AgentStatus,
    CreateAgentIdentity, CreateAgentProfile, CreateDomainEvent, CreateExecution, DomainEventRepo,
    ExecutionRepo, ExecutionStatus, SqliteDb, UpdateExecution,
};
use services::{AttentionService, WakeTurnConsumer};

async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    Arc::new(SqliteDb::new(pool))
}

async fn identity_with_profile(db: &SqliteDb, id: &str) -> String {
    let now = now_rfc3339();
    let profile_id = new_uuid_v4();
    AgentRepo::create_identity_with_profile(
        db,
        CreateAgentIdentity {
            id: id.to_owned(),
            name: "wake-turn-test".to_owned(),
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
            id: profile_id.clone(),
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
    profile_id
}

/// Insert a Project (the schema trigger creates its chat and setup binding),
/// promote the chat to ready, and bind the identity as the active responder.
async fn bound_project(db: &SqliteDb, identity_id: &str, profile_id: &str) -> (String, String) {
    let project_id = new_uuid_v4();
    let now = now_rfc3339();
    sqlx::query("INSERT INTO project (id, name, created_at, updated_at) VALUES (?, ?, ?, ?)")
        .bind(&project_id)
        .bind("wake-turn-project")
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .unwrap();
    let chat_id: String =
        sqlx::query_scalar("SELECT id FROM agent_chat WHERE kind = 'project' AND project_id = ?")
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    sqlx::query("UPDATE agent_chat SET status = 'ready' WHERE id = ?")
        .bind(&chat_id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE project_agent_binding
         SET identity_id = ?, profile_id = ?, state = 'active'
         WHERE project_id = ?",
    )
    .bind(identity_id)
    .bind(profile_id)
    .bind(&project_id)
    .execute(db.pool())
    .await
    .unwrap();
    (project_id, chat_id)
}

async fn append_event(db: &SqliteDb, event: CreateDomainEvent) {
    DomainEventRepo::append_event(db, event).await.unwrap();
}

fn wake_event(identity_id: &str, project_id: &str, incident_key: &str) -> CreateDomainEvent {
    let event_id = new_uuid_v4();
    CreateDomainEvent {
        id: event_id.clone(),
        event_type: "agent.wake.admitted".to_owned(),
        entity_type: "agent_wake".to_owned(),
        entity_id: incident_key.to_owned(),
        actor_type: "attention_projection".to_owned(),
        actor_id: None,
        scope_type: "project".to_owned(),
        scope_id: project_id.to_owned(),
        correlation_id: event_id,
        causation_id: None,
        causation_depth: 1,
        dedupe_key: Some(format!(
            "agent-wake-admitted:{identity_id}:project:{project_id}:{incident_key}"
        )),
        payload_json: serde_json::json!({
            "action": "wake_admitted",
            "identity_id": identity_id,
            "scope_type": "project",
            "scope_id": project_id,
            "incident_key": incident_key,
        })
        .to_string(),
        created_at: now_rfc3339(),
    }
}

#[tokio::test]
async fn admitted_wake_becomes_a_project_agent_turn() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;

    // Arm both consumers before any event exists: the first run initializes
    // the cursor past pre-existing history (upgrade fast-forward).
    let consumer = WakeTurnConsumer::new(Arc::clone(&db), "test-lease-owner");
    consumer.run_once(100).await.unwrap();
    let replay = WakeTurnConsumer::new(Arc::clone(&db), "other-lease-owner")
        .with_consumer_name("agent-wake-turns-replay");
    replay.run_once(100).await.unwrap();

    let incident_key = format!("attention:execution_failed:project:{project_id}:task:task-1");
    sqlx::query(
        "INSERT INTO attention_projection (
            id, attention_type, scope_type, scope_id, identity_id, source_event_id,
            priority, status, summary, details_json, dedupe_key, occurred_at,
            updated_at, recommended_action
         ) VALUES (?, 'execution_failed', 'project', ?, NULL, ?, 85, 'open',
                   'Task execution failed', '{}', ?, ?, ?, 'inspect_run')",
    )
    .bind(new_uuid_v4())
    .bind(&project_id)
    .bind({
        let source = new_uuid_v4();
        append_event(
            &db,
            CreateDomainEvent {
                id: source.clone(),
                event_type: "execution.failed".to_owned(),
                entity_type: "task".to_owned(),
                entity_id: "task-1".to_owned(),
                actor_type: "system".to_owned(),
                actor_id: None,
                scope_type: "project".to_owned(),
                scope_id: project_id.clone(),
                correlation_id: source.clone(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: Some(format!("execution-terminal:{}:failed", new_uuid_v4())),
                payload_json: "{}".to_owned(),
                created_at: now_rfc3339(),
            },
        )
        .await;
        source
    })
    .bind(&incident_key)
    .bind(now_rfc3339())
    .bind(now_rfc3339())
    .execute(db.pool())
    .await
    .unwrap();

    append_event(&db, wake_event(&identity_id, &project_id, &incident_key)).await;

    let run = consumer.run_once(100).await.unwrap();
    assert!(run.delivered_turns >= 1, "wake must deliver a turn");

    let (turn_count, status, responder): (i64, String, String) = sqlx::query_as(
        "SELECT COUNT(*), MAX(status), MAX(responder_identity_id)
         FROM agent_chat_turn_job WHERE chat_id = ? AND dedupe_key LIKE 'wake-turn:%'",
    )
    .bind(&chat_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(turn_count, 1);
    assert_eq!(status, "queued");
    assert_eq!(responder, identity_id);

    let message: (String, String) = sqlx::query_as(
        "SELECT author_type, content FROM agent_chat_message
         WHERE chat_id = ? ORDER BY sequence DESC LIMIT 1",
    )
    .bind(&chat_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(message.0, "system");
    assert!(message.1.contains("Task execution failed"));
    assert!(message.1.contains("inspect_run"));

    // Replay by a second consumer instance must not create a second turn.
    replay.run_once(100).await.unwrap();
    let turn_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_chat_turn_job WHERE chat_id = ? AND dedupe_key LIKE 'wake-turn:%'",
    )
    .bind(&chat_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(turn_count, 1, "replay must reuse the deduped turn");
}

#[tokio::test]
async fn baseline_activation_delivers_begin_execution_turn() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    let consumer = WakeTurnConsumer::new(Arc::clone(&db), "test-lease-owner");
    consumer.run_once(100).await.unwrap();

    let event_id = new_uuid_v4();
    append_event(
        &db,
        CreateDomainEvent {
            id: event_id.clone(),
            event_type: "project.execution_baseline.activated".to_owned(),
            entity_type: "execution_baseline".to_owned(),
            entity_id: "baseline-1".to_owned(),
            actor_type: "user".to_owned(),
            actor_id: Some("user-1".to_owned()),
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: event_id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("baseline-activation-1".to_owned()),
            payload_json: serde_json::json!({
                "result": {"baseline_id": "baseline-1", "revision_id": "revision-1"},
            })
            .to_string(),
            created_at: now_rfc3339(),
        },
    )
    .await;

    let run = consumer.run_once(100).await.unwrap();
    assert!(run.delivered_turns >= 1);

    let (status, content): (String, String) = sqlx::query_as(
        "SELECT job.status, message.content
         FROM agent_chat_turn_job AS job
         JOIN agent_chat_message AS message ON message.id = job.triggering_message_id
         WHERE job.chat_id = ? AND job.dedupe_key LIKE 'baseline-turn:%'",
    )
    .bind(&chat_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(status, "queued");
    assert!(content.contains("baseline-1"));
    assert!(content.contains("Begin execution"));
}

/// The whole autonomy loop, end to end: a Task execution fails → the durable
/// `execution.failed` event → Attention projects an incident and admits a
/// wake for the Project Agent binding → the wake consumer queues a turn on
/// the Project chat. No user message anywhere.
#[tokio::test]
async fn failed_execution_wakes_the_project_agent_end_to_end() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    let consumer = WakeTurnConsumer::new(Arc::clone(&db), "loop-lease-owner");
    consumer.run_once(100).await.unwrap();

    let task_id = new_uuid_v4();
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO task (id, project_id, title, status, created_at, updated_at)
         VALUES (?, ?, 'wake loop task', 'in_progress', ?, ?)",
    )
    .bind(&task_id)
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .unwrap();

    let execution_id = new_uuid_v4();
    ExecutionRepo::create(
        &*db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: None,
            role: "worker".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
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
    ExecutionRepo::update(
        &*db,
        UpdateExecution {
            id: execution_id.clone(),
            status: Some(ExecutionStatus::Failed),
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some(Some("gemini exited with status 1".to_owned())),
            executor_config_snapshot_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();

    AttentionService::new(Arc::clone(&db))
        .project_once(100)
        .await
        .unwrap();
    let wake_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'agent.wake.admitted'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        wake_count, 1,
        "failed execution must admit exactly one wake"
    );

    consumer.run_once(100).await.unwrap();

    let (status, responder, content): (String, String, String) = sqlx::query_as(
        "SELECT job.status, job.responder_identity_id, message.content
         FROM agent_chat_turn_job AS job
         JOIN agent_chat_message AS message ON message.id = job.triggering_message_id
         WHERE job.chat_id = ? AND job.dedupe_key LIKE 'wake-turn:%'",
    )
    .bind(&chat_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(status, "queued");
    assert_eq!(responder, identity_id);
    assert!(content.contains("Task execution failed"));
}

#[tokio::test]
async fn wake_for_replaced_binding_is_skipped_with_receipt() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;

    let consumer = WakeTurnConsumer::new(Arc::clone(&db), "test-lease-owner");
    consumer.run_once(100).await.unwrap();

    // Admit a wake for a different identity than the current binding holder.
    let stranger = new_uuid_v4();
    identity_with_profile(&db, &stranger).await;
    append_event(&db, wake_event(&stranger, &project_id, "attention:x:1")).await;

    let run = consumer.run_once(100).await.unwrap();
    assert_eq!(run.delivered_turns, 0);
    assert!(run.processed_events >= 1, "skipped wake still checkpoints");

    let turn_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_turn_job WHERE chat_id = ?")
            .bind(&chat_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(turn_count, 0);
}
