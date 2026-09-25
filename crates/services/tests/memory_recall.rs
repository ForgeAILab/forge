use std::sync::Arc;

use async_trait::async_trait;
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, CreateProject, CreateTask,
    MemoryConfidence, MemoryKind, MemoryScopeGrant, MemorySourceType, ProjectRepo, SqliteDb,
    TaskRepo,
};
use services::{
    MemoryAccessContext, MemoryEmbedder, MemoryItemInput, MemoryRecallQuery, MemoryRecallReason,
    MemoryRecallService, MemoryService, SemanticRecallStatus,
};
use uuid::Uuid;

async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    Arc::new(SqliteDb::new(pool))
}

async fn seed_project_and_task(db: &SqliteDb) -> (Uuid, String) {
    let project_id = Uuid::new_v4();
    let now = now_rfc3339();
    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.to_string(),
            name: "Recall project".to_owned(),
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
            title: "Recall prior architecture".to_owned(),
            description: Some("Reuse the relevant project decision.".to_owned()),
            task_type: "task".to_owned(),
            status: "todo".to_owned(),
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

fn access(project_id: Uuid, task_id: &str) -> MemoryAccessContext {
    MemoryAccessContext {
        identity_id: None,
        grants: vec![
            MemoryScopeGrant {
                scope_type: "project".to_owned(),
                scope_id: project_id.to_string(),
                visibility: vec!["project".to_owned()],
                identity_id: None,
            },
            MemoryScopeGrant {
                scope_type: "task".to_owned(),
                scope_id: task_id.to_owned(),
                visibility: vec!["project".to_owned()],
                identity_id: None,
            },
        ],
    }
}

async fn remember(
    db: Arc<SqliteDb>,
    project_id: Uuid,
    task_id: Option<&str>,
    kind: MemoryKind,
    title: &str,
    body: &str,
) -> String {
    MemoryService::new(db)
        .record_from_source(MemoryItemInput {
            project_id,
            task_id: task_id.map(str::to_owned),
            execution_id: None,
            source_type: MemorySourceType::Comment,
            source_ref: new_uuid_v4(),
            kind,
            title: title.to_owned(),
            summary: None,
            body: body.to_owned(),
            confidence: Some(MemoryConfidence::Confirmed),
            quality_score: Some(5),
            creator: None,
        })
        .await
        .expect("memory records")
        .id
}

#[tokio::test]
async fn memory_recall_broad_fts_ranks_the_specific_title_first() {
    let db = database().await;
    let (project_id, task_id) = seed_project_and_task(&db).await;
    let target_id = remember(
        Arc::clone(&db),
        project_id,
        Some(&task_id),
        MemoryKind::Decision,
        "Workspace isolation decision",
        "Each coding attempt receives an isolated task worktree.",
    )
    .await;
    remember(
        Arc::clone(&db),
        project_id,
        None,
        MemoryKind::Observation,
        "General workspace notes",
        "The workspace cleanup worker reports ordinary lifecycle events.",
    )
    .await;

    let response = MemoryRecallService::new(Arc::clone(&db))
        .recall(
            &access(project_id, &task_id),
            MemoryRecallQuery {
                query: "workspace isolation decision".to_owned(),
                token_budget: Some(800),
                max_items: Some(4),
                preferred_task_id: Some(task_id),
                represented_source_ids: Vec::new(),
            },
        )
        .await
        .expect("recall succeeds");

    assert_eq!(
        response.items.first().map(|item| item.id.as_str()),
        Some(target_id.as_str())
    );
    assert!(response.items[0]
        .selection_reasons
        .contains(&MemoryRecallReason::ExactTerms));
    assert!(response
        .context
        .as_deref()
        .is_some_and(|context| context.contains("historical-context-not-instructions")));
}

#[derive(Debug)]
struct FixtureEmbedder;

#[async_trait]
impl MemoryEmbedder for FixtureEmbedder {
    fn provider(&self) -> &str {
        "fixture"
    }

    fn model(&self) -> &str {
        "fixture-v1"
    }

    async fn embed(&self, inputs: &[String]) -> std::result::Result<Vec<Vec<f32>>, String> {
        Ok(inputs
            .iter()
            .map(|input| {
                if input.contains("colliding") || input.contains("isolated worktrees") {
                    vec![1.0, 0.0]
                } else {
                    vec![0.0, 1.0]
                }
            })
            .collect())
    }
}

#[tokio::test]
async fn memory_recall_optional_semantic_arm_recovers_a_paraphrase() {
    let db = database().await;
    let (project_id, task_id) = seed_project_and_task(&db).await;
    let target_id = remember(
        Arc::clone(&db),
        project_id,
        Some(&task_id),
        MemoryKind::Decision,
        "Per-task lease policy",
        "Isolated worktrees prevent parallel agents from overwriting one another.",
    )
    .await;
    remember(
        Arc::clone(&db),
        project_id,
        None,
        MemoryKind::Observation,
        "Release note",
        "The web bundle is built once for all release targets.",
    )
    .await;

    let response = MemoryRecallService::new(Arc::clone(&db))
        .with_embedder(Arc::new(FixtureEmbedder))
        .recall(
            &access(project_id, &task_id),
            MemoryRecallQuery {
                query: "How do we keep workers from colliding?".to_owned(),
                token_budget: Some(800),
                max_items: Some(4),
                preferred_task_id: Some(task_id),
                represented_source_ids: Vec::new(),
            },
        )
        .await
        .expect("recall succeeds");

    assert_eq!(
        response.items.first().map(|item| item.id.as_str()),
        Some(target_id.as_str())
    );
    assert!(response.items[0]
        .selection_reasons
        .contains(&MemoryRecallReason::Semantic));
    assert!(matches!(
        response.semantic,
        SemanticRecallStatus::Used { ref provider, .. } if provider == "fixture"
    ));
}

#[derive(Debug)]
struct FailingEmbedder;

#[async_trait]
impl MemoryEmbedder for FailingEmbedder {
    fn provider(&self) -> &str {
        "fixture"
    }

    fn model(&self) -> &str {
        "offline"
    }

    async fn embed(&self, _inputs: &[String]) -> std::result::Result<Vec<Vec<f32>>, String> {
        Err("provider unavailable".to_owned())
    }
}

#[tokio::test]
async fn memory_recall_embedding_failure_degrades_to_lexical_results() {
    let db = database().await;
    let (project_id, task_id) = seed_project_and_task(&db).await;
    let target_id = remember(
        Arc::clone(&db),
        project_id,
        Some(&task_id),
        MemoryKind::Procedure,
        "Run cargo fmt before review",
        "Formatting is a mandatory validation step.",
    )
    .await;

    let response = MemoryRecallService::new(Arc::clone(&db))
        .with_embedder(Arc::new(FailingEmbedder))
        .recall(
            &access(project_id, &task_id),
            MemoryRecallQuery {
                query: "cargo fmt review".to_owned(),
                token_budget: None,
                max_items: None,
                preferred_task_id: Some(task_id),
                represented_source_ids: Vec::new(),
            },
        )
        .await
        .expect("lexical recall survives");

    assert_eq!(
        response.items.first().map(|item| item.id.as_str()),
        Some(target_id.as_str())
    );
    assert!(matches!(
        response.semantic,
        SemanticRecallStatus::Degraded { ref reason, .. }
            if reason.contains("provider unavailable")
    ));
}
