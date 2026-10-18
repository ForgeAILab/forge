use crate::test_services as services;
use std::sync::Arc;

use api_types::{
    ExecutionOutboxArtifact, ExecutionOutboxEntry, ExecutionOutboxEvidenceKind,
    ExecutionOutboxWorklogKind,
};
use db::{
    CreateExecution, CreateProject, CreateTask, ExecutionRepo, ExecutionStatus, ProjectRepo,
    SqliteDb, TaskRepo,
};
use services::task_service::execution::ExecutionOutboxInput;
use services_agents::CoordinationToolProvider;

const TASK_ID: &str = "11111111-1111-4111-8111-111111111111";
const PROJECT_ID: &str = "22222222-2222-4222-8222-222222222222";
const EXECUTION_ID: &str = "55555555-5555-4555-8555-555555555555";
const NOW: &str = "2026-09-29T00:00:00Z";

async fn database() -> Arc<SqliteDb> {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: PROJECT_ID.to_owned(),
            name: "Owner outbox fixture".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .unwrap();
    TaskRepo::create(
        &*db,
        CreateTask {
            id: TASK_ID.to_owned(),
            project_id: PROJECT_ID.to_owned(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Owner outbox".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "in_progress".to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .unwrap();
    ExecutionRepo::create(
        &*db,
        CreateExecution {
            id: EXECUTION_ID.to_owned(),
            task_id: TASK_ID.to_owned(),
            agent_id: None,
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
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
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .unwrap();
    db
}

#[tokio::test]
async fn owner_harvested_outbox_ingests_bytes_without_owner_paths_and_replays_once() {
    let db = database().await;
    let provider = CoordinationToolProvider::new(Arc::clone(&db));
    let media = tempfile::tempdir().unwrap();
    provider.set_media_root(media.path().to_path_buf());
    let input = ExecutionOutboxInput {
        task_id: TASK_ID,
        execution_id: EXECUTION_ID,
        agent_id: "outbox-agent",
        role: Some("executor"),
        worktree_path: "/owner-only/nonexistent/worktree",
    };
    let entries = vec![
        ExecutionOutboxEntry::Worklog {
            position: "2".to_owned(),
            kind: ExecutionOutboxWorklogKind::Validation,
            summary: "Owner validation passed".to_owned(),
        },
        ExecutionOutboxEntry::Evidence {
            position: "3".to_owned(),
            kind: ExecutionOutboxEvidenceKind::Report,
            caption: "Owner proof".to_owned(),
            path: Some("/owner-only/nonexistent/report.md".to_owned()),
            content: None,
            artifact: Some(ExecutionOutboxArtifact {
                filename: "report.md".to_owned(),
                content_type: "text/markdown".to_owned(),
                bytes: b"owner evidence\n".to_vec(),
            }),
        },
    ];
    for _ in 0..2 {
        let report = provider
            .ingest_execution_outbox_entries(&input, entries.clone())
            .await;
        assert!(report.rejected.is_empty(), "{report:?}");
        assert_eq!(report.worklog_entries, 1);
        assert_eq!(report.evidence_items, 1);
    }
    let comments = db::TaskCommentRepo::list_comments(
        &*db,
        TASK_ID,
        db::PageRequest {
            cursor: None,
            limit: 50,
            include_total: false,
            sort_by: db::SortBy::CreatedAt,
            sort_order: db::SortOrder::Asc,
        },
    )
    .await
    .unwrap()
    .items;
    assert_eq!(comments.len(), 2);
    assert!(comments.iter().all(|comment| {
        comment.execution_id.as_deref() == Some(EXECUTION_ID)
            && comment.role.as_deref() == Some("executor")
            && comment.author_id.as_deref() == Some("outbox-agent")
    }));
    let stored = db::TaskMediaRepo::list_active_media_for_task(&*db, TASK_ID)
        .await
        .unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(
        std::fs::read(media.path().join(&stored[0].storage_key)).unwrap(),
        b"owner evidence\n"
    );
}
