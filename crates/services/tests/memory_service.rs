use std::sync::Arc;

use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentChatRepo, CreateDomainEvent,
    CreateExecution, CreateProject, CreateReview, CreateTask, DomainEventRepo, ExecutionRepo,
    ExecutionStatus, MemoryConfidence, MemoryKind, MemorySourceType, ProjectRepo, ReviewRepo,
    ReviewStatus, SqliteDb, TaskRepo,
};
use serde_json::json;
use services::{AgentChatMemoryConsumer, MemoryItemInput, MemoryService, TaskService};
use uuid::Uuid;

async fn sqlite_db() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    Arc::new(SqliteDb::new(pool))
}

async fn seed_project_and_task(db: &SqliteDb, status: &str) -> (Uuid, String) {
    let project_id = Uuid::new_v4();
    let now = now_rfc3339();
    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.to_string(),
            name: "Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    let task_id = new_uuid_v4();
    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.clone(),
            project_id: project_id.to_string(),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "Task".to_owned(),
            description: Some("Task description".to_owned()),
            task_type: "task".to_owned(),
            status: status.to_owned(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("task creates");
    (project_id, task_id)
}

#[tokio::test]
async fn agent_chat_memory_consumer_preserves_upgrade_cursor_ignores_legacy_lease_and_handles_once()
{
    let db = sqlite_db().await;
    let (project_id, _task_id) = seed_project_and_task(&db, "review").await;
    let message_id = new_uuid_v4();
    let event_id = new_uuid_v4();
    let now = "2026-08-12T00:00:00.000Z";
    let chat_id = AgentChatRepo::get_project_chat(&*db, &project_id.to_string())
        .await
        .expect("project chat reads")
        .expect("project creation provisions the singular chat")
        .id;

    let old_event = DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "agent_chat.message.admitted".to_owned(),
            entity_type: "agent_chat_message".to_owned(),
            entity_id: "deleted-before-upgrade".to_owned(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "agent_chat".to_owned(),
            scope_id: chat_id.clone(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: None,
            payload_json: "{}".to_owned(),
            created_at: now.to_owned(),
        },
    )
    .await
    .unwrap();
    // Simulate an upgrade from the lease/receipt consumer. The new runtime
    // must reuse this exact stable cursor and begin strictly after it.
    let pre_upgrade_cursor: i64 =
        sqlx::query_scalar("SELECT COALESCE(MAX(sequence), 0) FROM domain_event")
            .fetch_one(db.pool())
            .await
            .expect("pre-upgrade head loads");
    sqlx::query(
        "INSERT INTO event_consumer_cursor (consumer_name, last_sequence, updated_at)
         VALUES (?, ?, ?)",
    )
    .bind(services::memory_consumer_name())
    .bind(pre_upgrade_cursor)
    .bind(now)
    .execute(db.pool())
    .await
    .expect("legacy cursor seeds");

    // A wanted event at/below the saved cursor must never be revisited, even
    // though its source has been deleted. A live legacy lease after the cursor
    // must neither block nor duplicate projection.
    assert!(old_event.sequence <= pre_upgrade_cursor);
    let wanted = DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: event_id.clone(),
            event_type: "agent_chat.message.admitted".to_owned(),
            entity_type: "agent_chat_message".to_owned(),
            entity_id: message_id.clone(),
            actor_type: "user".to_owned(),
            actor_id: None,
            // V060's historical scope check predates the singular Chat
            // vocabulary; the projection keys by entity type and chat id
            // while the follow-up migration expands this value to agent_chat.
            scope_type: "project".to_owned(),
            scope_id: chat_id.clone(),
            correlation_id: event_id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!("agent-chat-memory-replay:{message_id}")),
            payload_json: "{}".to_owned(),
            created_at: now.to_owned(),
        },
    )
    .await
    .expect("event appends");
    sqlx::query(
        "INSERT INTO event_processing_lease (consumer_name, event_sequence, lease_owner,
        leased_until, attempts, updated_at) VALUES (?, ?, 'legacy', '2999-01-01T00:00:00Z', 1, ?)",
    )
    .bind(services::memory_consumer_name())
    .bind(wanted.sequence)
    .bind(now)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO agent_chat_message (
            id, chat_id, sequence, author_type, content, status, correlation_id,
            source_type, source_metadata_json, created_at
         ) VALUES (?, ?, 0, 'user', ?, 'complete', ?, 'native', '{}', ?)",
    )
    .bind(&message_id)
    .bind(&chat_id)
    .bind("durable room message")
    .bind(&event_id)
    .bind(now)
    .execute(db.pool())
    .await
    .expect("message inserts after source recovery");

    let second = AgentChatMemoryConsumer::new(Arc::clone(&db))
        .run_once(10)
        .await
        .expect("event projects after the legacy cursor");
    assert_eq!(second, 1);
    let third = AgentChatMemoryConsumer::new(Arc::clone(&db))
        .run_once(10)
        .await
        .expect("cursor suppresses duplicate");
    assert_eq!(third, 0);
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM memory_item WHERE source_type = 'agent_chat' AND json_extract(metadata_json, '$.source_ref') = ?",
    )
    .bind(&message_id)
    .fetch_one(db.pool())
    .await
    .expect("projected source count loads");
    assert_eq!(count, 1);
    let canonical_scope_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM memory_item
         WHERE source_type = 'agent_chat' AND scope_type = 'agent_chat' AND scope_id = ?",
    )
    .bind(&chat_id)
    .fetch_one(db.pool())
    .await
    .expect("Agent Chat memory scope count loads");
    assert_eq!(canonical_scope_count, 1);
    let dead: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM worker_dead_letter")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(dead, 0, "wanted event below the cursor is not revisited");
    let leases: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM event_processing_lease WHERE consumer_name = ?")
            .bind(services::memory_consumer_name())
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(
        leases, 1,
        "generic source does not touch seeded legacy leases"
    );
}

#[tokio::test]
async fn agent_chat_memory_consumer_skips_unsubscribed_failure_events_without_projection() {
    let db = sqlite_db().await;
    let (project_id, _task_id) = seed_project_and_task(&db, "review").await;
    let chat_id = AgentChatRepo::get_project_chat(&*db, &project_id.to_string())
        .await
        .expect("project chat reads")
        .expect("project chat exists")
        .id;
    let job_id = new_uuid_v4();
    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "agent_chat.turn.failed".to_owned(),
            entity_type: "agent_chat_turn_job".to_owned(),
            entity_id: job_id.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "agent_chat".to_owned(),
            scope_id: chat_id,
            correlation_id: "failure-correlation".to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!("failure-event:{job_id}")),
            payload_json: json!({
                "status": "failed",
                "error_code": "adapter_failed",
                "error_message": "bounded failure",
            })
            .to_string(),
            created_at: now_rfc3339(),
        },
    )
    .await
    .expect("failure event appends");

    let consumer = AgentChatMemoryConsumer::new(Arc::clone(&db));
    assert_eq!(consumer.run_once(10).await.expect("failure event skips"), 0);
    assert_eq!(consumer.run_once(10).await.expect("replay is empty"), 0);
}

#[tokio::test]
async fn agent_chat_memory_consumer_deleted_sources_are_terminal_without_strikes() {
    let db = sqlite_db().await;
    let (project_id, _) = seed_project_and_task(&db, "review").await;
    let chat_id = AgentChatRepo::get_project_chat(&*db, &project_id.to_string())
        .await
        .unwrap()
        .unwrap()
        .id;
    for scope_id in [chat_id, "deleted-chat".to_owned()] {
        DomainEventRepo::append_event(
            &*db,
            CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: "agent_chat.message.admitted".to_owned(),
                entity_type: "agent_chat_message".to_owned(),
                entity_id: new_uuid_v4(),
                actor_type: "system".to_owned(),
                actor_id: None,
                scope_type: "agent_chat".to_owned(),
                scope_id,
                correlation_id: new_uuid_v4(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: None,
                payload_json: "{}".to_owned(),
                created_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
    }
    let consumer = AgentChatMemoryConsumer::new(Arc::clone(&db));
    assert_eq!(consumer.run_once(10).await.unwrap(), 2);
    assert_eq!(consumer.run_once(10).await.unwrap(), 0);
    let attempts: Vec<i64> =
        sqlx::query_scalar("SELECT attempts FROM worker_dead_letter ORDER BY source_key")
            .fetch_all(db.pool())
            .await
            .unwrap();
    assert_eq!(attempts, [0, 0]);
}

#[tokio::test]
async fn agent_chat_memory_consumer_database_read_errors_are_transient() {
    let db = sqlite_db().await;
    let (project_id, _) = seed_project_and_task(&db, "review").await;
    let chat_id = AgentChatRepo::get_project_chat(&*db, &project_id.to_string())
        .await
        .unwrap()
        .unwrap()
        .id;
    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "agent_chat.message.admitted".to_owned(),
            entity_type: "agent_chat_message".to_owned(),
            entity_id: new_uuid_v4(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "agent_chat".to_owned(),
            scope_id: chat_id,
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: None,
            payload_json: "{}".to_owned(),
            created_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    sqlx::query("ALTER TABLE agent_chat_message RENAME TO unavailable_message")
        .execute(db.pool())
        .await
        .unwrap();
    for _ in 0..10 {
        assert_eq!(
            AgentChatMemoryConsumer::new(Arc::clone(&db))
                .run_once(1)
                .await
                .unwrap(),
            0
        );
    }
    let attempts: i64 =
        sqlx::query_scalar("SELECT retry_attempts FROM worker_health WHERE worker_name = ?")
            .bind(services::memory_consumer_name())
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(attempts, 0);
    let dead: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM worker_dead_letter")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(dead, 0);
    sqlx::query("ALTER TABLE unavailable_message RENAME TO agent_chat_message")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        AgentChatMemoryConsumer::new(Arc::clone(&db))
            .run_once(1)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn memory_indexing_failure_does_not_fail_source_operation() {
    let db = sqlite_db().await;
    let (_project_id, task_id) = seed_project_and_task(&db, "review").await;
    let now = now_rfc3339();
    let execution_id = new_uuid_v4();
    ExecutionRepo::create(
        &*db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: None,
            role: "reviewer".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("review summary".to_owned()),
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
    .expect("execution creates");
    let review_id = new_uuid_v4();
    ReviewRepo::create(
        &*db,
        CreateReview {
            id: review_id.clone(),
            task_id: task_id.clone(),
            execution_id,
            attempt_number: 1,
            status: ReviewStatus::AwaitingHuman,
            step_results_json: json!({ "auditor": { "verdict": "pass" } }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("review creates");

    sqlx::query("DROP TABLE memory_item")
        .execute(db.pool())
        .await
        .expect("memory table drops");

    let service = TaskService::new_for_test(Arc::clone(&db), Arc::new(events::EventBus::new(16)));
    service
        .approve_review(task_id.clone())
        .await
        .expect("source operation succeeds");

    let review = ReviewRepo::get_by_id(&*db, &review_id)
        .await
        .expect("review loads")
        .expect("review exists");
    assert_eq!(review.status, ReviewStatus::Passed);
}

#[tokio::test]
async fn memory_search_token_budget_shapes_layers() {
    let db = sqlite_db().await;
    let (project_id, task_id) = seed_project_and_task(&db, "in_progress").await;
    let service = MemoryService::new(Arc::clone(&db));
    service
        .record_from_source(MemoryItemInput {
            project_id,
            task_id: Some(task_id),
            execution_id: None,
            source_type: MemorySourceType::Comment,
            source_ref: new_uuid_v4(),
            kind: MemoryKind::Comment,
            title: "Layered memory title".to_owned(),
            summary: Some("Layered summary".to_owned()),
            body: "Layered body with full detail".to_owned(),
            confidence: Some(MemoryConfidence::Confirmed),
            quality_score: None,
            creator: None,
        })
        .await
        .expect("memory records");

    let (layer_one, _, _) = service
        .search(project_id, "Layered".to_owned(), None, Some(199), 10, None)
        .await
        .expect("layer one search succeeds");
    assert!(layer_one[0].summary.is_none());
    assert!(layer_one[0].body.is_none());
    assert!(layer_one[0].references.is_none());

    let (layer_two, _, _) = service
        .search(project_id, "Layered".to_owned(), None, Some(1000), 10, None)
        .await
        .expect("layer two search succeeds");
    assert_eq!(layer_two[0].summary.as_deref(), Some("Layered summary"));
    assert!(layer_two[0].body.is_none());
    assert!(layer_two[0].references.is_some());

    let (layer_three, _, _) = service
        .search(project_id, "Layered".to_owned(), None, Some(1001), 10, None)
        .await
        .expect("layer three search succeeds");
    assert_eq!(
        layer_three[0].body.as_deref(),
        Some("Layered body with full detail")
    );
}
