use std::sync::Arc;

use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentChatRepo, CommentAuthorType,
    CreateDomainEvent, CreateExecution, CreateProject, CreateReview, CreateTask, CreateTaskComment,
    CreateTransitionLog, DomainEventRepo, ExecutionRepo, ExecutionStatus, MemoryConfidence,
    MemoryKind, MemorySourceType, ProjectRepo, ReviewRepo, ReviewStatus, SqliteDb, TaskCommentRepo,
    TaskRepo, TransitionLogRepo,
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
            repo_id: None,
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
async fn agent_chat_memory_consumer_replays_expired_lease_and_deduplicates_after_restart() {
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

    // Append the durable event before its source row to simulate a process
    // crash between event admission and projection. The first consumer run
    // claims the event but cannot complete it, leaving only an expiring lease.
    DomainEventRepo::append_event(
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
    let _first = AgentChatMemoryConsumer::new(Arc::clone(&db), "consumer-before-crash")
        .run_once(10)
        .await
        .expect("failed projection is retryable");
    // Project/task setup may have produced unrelated durable events before
    // this source event. They are checkpointed by the shared ledger consumer,
    // but the missing message itself must remain leased for retry.
    let first_receipt: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM event_projection_receipt
         WHERE consumer_name = ? AND event_id = ?",
    )
    .bind(services::memory_consumer_name())
    .bind(&event_id)
    .fetch_one(db.pool())
    .await
    .expect("missing source remains uncheckpointed");
    assert_eq!(first_receipt, 0);

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
    sqlx::query(
        "UPDATE event_processing_lease
         SET leased_until = '2000-01-01T00:00:00Z'
         WHERE consumer_name = ?",
    )
    .bind(services::memory_consumer_name())
    .execute(db.pool())
    .await
    .expect("lease expires");

    let second = AgentChatMemoryConsumer::new(Arc::clone(&db), "consumer-after-restart")
        .run_once(10)
        .await
        .expect("expired event replays");
    assert_eq!(second, 1);
    let third = AgentChatMemoryConsumer::new(Arc::clone(&db), "consumer-third-process")
        .run_once(10)
        .await
        .expect("receipt suppresses duplicate");
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
}

#[tokio::test]
async fn agent_chat_memory_consumer_checkpoints_failure_events_without_replaying_them() {
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

    let consumer = AgentChatMemoryConsumer::new(Arc::clone(&db), "failure-consumer");
    assert!(consumer.run_once(10).await.expect("failure event consumes") >= 1);
    assert_eq!(consumer.run_once(10).await.expect("replay is empty"), 0);
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

    let service = TaskService::new(Arc::clone(&db), Arc::new(events::EventBus::new(16)));
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
async fn memory_backfill_all_is_idempotent() {
    let db = sqlite_db().await;
    let (project_id, task_id) = seed_project_and_task(&db, "in_progress").await;
    let now = now_rfc3339();
    let execution_id = new_uuid_v4();
    ExecutionRepo::create(
        &*db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: None,
            role: "coder".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("execution summary".to_owned()),
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
    ReviewRepo::create(
        &*db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            execution_id: execution_id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Failed,
            step_results_json: json!({ "auditor": { "verdict": "fail", "reason": "bug" } })
                .to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("review creates");
    TaskCommentRepo::create_comment(
        &*db,
        CreateTaskComment {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            author_type: CommentAuthorType::System,
            author_id: None,
            author_name: "Forge".to_owned(),
            content: "comment content".to_owned(),
            execution_id: None,
            role: None,
            worklog_kind: None,
            idempotency_key: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("comment creates");
    TransitionLogRepo::insert(
        &*db,
        CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            from_state: "review".to_owned(),
            to_state: "merge_failed".to_owned(),
            trigger_name: Some("review_failed".to_owned()),
            triggered_by: "system".to_owned(),
            trigger_reason: "review failed".to_owned(),
            hook_results_json: None,
            rejection: true,
            created_at: now.clone(),
        },
    )
    .await
    .expect("transition creates");
    let first = MemoryService::backfill_all(Arc::clone(&db))
        .await
        .expect("first backfill succeeds");
    let second = MemoryService::backfill_all(Arc::clone(&db))
        .await
        .expect("second backfill succeeds");

    assert_eq!(first.indexed, 4);
    assert_eq!(first.skipped, 0);
    assert_eq!(second.indexed, 0);
    assert_eq!(second.skipped, 4);
    let count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM memory_item WHERE project_id = ?")
            .bind(project_id.to_string())
            .fetch_one(db.pool())
            .await
            .expect("memory count loads");
    assert_eq!(count, 4);
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
