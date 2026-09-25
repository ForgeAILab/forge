use std::sync::Arc;

use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentContextScopeRepo, AgentRepo,
    AgentStatus, CreateAgentContextScope, CreateAgentIdentity, CreateAgentProfile, CreateProject,
    MemoryItem, MemoryRepository, MemoryScopeGrant, ProjectRepo, SqliteDb,
};
use serde_json::json;
use services::{
    memory_source::{
        ForgeMemoryQuery, ForgeMemoryRecallQuery, ForgeMemorySource, MemorySourceBindingInput,
    },
    MemoryAccessContext,
};
use uuid::Uuid;

#[tokio::test]
async fn memory_source_falls_back_to_bounded_recall_for_natural_language_queries() {
    let fixture = memory_fixture().await;

    let search = fixture
        .source
        .search(ForgeMemoryQuery {
            // The exact FTS arm cannot match because "why" is not stored.
            // Recall removes the stop word and combines the salient terms.
            query: "why task leases prevent collisions".to_owned(),
            limit: 5,
            represented_source_ids: Vec::new(),
            cursor: None,
        })
        .await
        .expect("memory search succeeds");

    assert!(!search.has_more);
    assert!(search.next_cursor.is_none());
    assert_eq!(search.records[0].id, fixture.target_id);
    assert_eq!(
        search.records[0].source_ref.as_deref(),
        Some(fixture.target_source_ref.as_str())
    );

    let recall = fixture
        .source
        .recall(ForgeMemoryRecallQuery {
            query: "why task leases prevent collisions".to_owned(),
            limit: 5,
            represented_source_ids: Vec::new(),
            not_after: None,
        })
        .await
        .expect("explicit recall succeeds");

    assert_eq!(recall.records[0].record.id, fixture.target_id);
    assert!(recall.records[0]
        .selection_reason
        .contains("all 4 salient query terms matched"));
    assert_eq!(
        recall.query_terms,
        vec![
            "task".to_owned(),
            "leases".to_owned(),
            "prevent".to_owned(),
            "collisions".to_owned(),
        ]
    );
    assert!(recall.candidate_count >= 2);
}

#[tokio::test]
async fn memory_recall_freezes_candidates_to_the_admission_timestamp() {
    let fixture = memory_fixture().await;

    let recall = fixture
        .source
        .recall(ForgeMemoryRecallQuery {
            query: "task policy checklist".to_owned(),
            limit: 5,
            represented_source_ids: Vec::new(),
            not_after: Some("2026-06-01T00:00:00Z".to_owned()),
        })
        .await
        .expect("recall succeeds");

    assert!(recall
        .records
        .iter()
        .all(|record| { record.record.created_at.as_str() <= "2026-06-01T00:00:00Z" }));
    assert!(!recall
        .records
        .iter()
        .any(|record| record.record.title == "Task policy"));
}

#[tokio::test]
async fn memory_recall_suppresses_sources_already_represented_by_active_context() {
    let fixture = memory_fixture().await;

    let recall = fixture
        .source
        .recall(ForgeMemoryRecallQuery {
            query: "task leases prevent collisions".to_owned(),
            limit: 5,
            represented_source_ids: vec![fixture.target_source_ref.clone()],
            not_after: None,
        })
        .await
        .expect("recall succeeds");

    assert!(!recall
        .records
        .iter()
        .any(|record| record.record.id == fixture.target_id));
    assert!(recall
        .deduplicated_source_ids
        .iter()
        .any(|source_id| source_id == &fixture.target_source_ref));
}

struct MemoryFixture {
    source: ForgeMemorySource<SqliteDb>,
    target_id: Uuid,
    target_source_ref: String,
}

async fn memory_fixture() -> MemoryFixture {
    let db = Arc::new(sqlite_db().await);
    let now = now_rfc3339();
    let project_id = new_uuid_v4();

    ProjectRepo::create(
        db.as_ref(),
        CreateProject {
            id: project_id.clone(),
            name: "Lightweight recall".to_owned(),
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

    let identity_id = new_uuid_v4();
    AgentRepo::create_identity_with_profile(
        db.as_ref(),
        CreateAgentIdentity {
            id: identity_id.clone(),
            name: "memory-recall-agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            account_permission_ceiling: json!({"permissions": ["read_project"]}).to_string(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: new_uuid_v4(),
            identity_id: identity_id.clone(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: json!({"allowed": ["read_project"]}).to_string(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("identity creates");

    let context_scope_id = new_uuid_v4();
    AgentContextScopeRepo::create_context_scope(
        db.as_ref(),
        CreateAgentContextScope {
            id: context_scope_id.clone(),
            identity_id: identity_id.clone(),
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            project_id: Some(project_id.clone()),
            task_id: None,
            task_role: None,
            workspace_access: "deny".to_owned(),
            workspace_path: None,
            authority_json: json!({
                "scope": {"type": "project", "project_id": project_id}
            })
            .to_string(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("context scope creates");

    let target_source_ref = "memory:task-lease-isolation".to_owned();
    let target = memory_item(
        &project_id,
        "observation",
        10,
        "Task leases prevent workspace collisions",
        Some("Each task receives an isolated workspace lease."),
        "Task leases prevent collisions by keeping agent changes in separate worktrees.",
        &target_source_ref,
        "2026-01-01T00:00:00Z",
    );
    let weak_high_authority = memory_item(
        &project_id,
        "decision",
        200,
        "Task policy",
        Some("The task owner maintains a checklist."),
        "Task metadata is reviewed before dispatch.",
        "memory:general-task-policy",
        "2026-09-25T00:00:00Z",
    );
    for item in [&target, &weak_high_authority] {
        MemoryRepository::insert_memory_item(db.as_ref(), item)
            .await
            .expect("memory inserts");
    }

    let source = ForgeMemorySource::bind(
        Arc::clone(&db),
        MemorySourceBindingInput {
            binding_id: Uuid::new_v4(),
            identity_id: Uuid::parse_str(&identity_id).expect("identity is UUID"),
            context_scope_id: Uuid::parse_str(&context_scope_id).expect("scope is UUID"),
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            account_id: None,
            project_id: Some(project_id.clone()),
            task_id: None,
            policy_revision: "memory-recall-v1".to_owned(),
            access: MemoryAccessContext {
                identity_id: Some(identity_id),
                grants: vec![MemoryScopeGrant {
                    scope_type: "project".to_owned(),
                    scope_id: project_id,
                    visibility: vec!["project".to_owned()],
                    identity_id: None,
                }],
            },
        },
    )
    .await
    .expect("memory source binds");

    MemoryFixture {
        source,
        target_id: Uuid::parse_str(&target.id).expect("target id is UUID"),
        target_source_ref,
    }
}

async fn sqlite_db() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    SqliteDb::new(pool)
}

#[allow(clippy::too_many_arguments)]
fn memory_item(
    project_id: &str,
    authority: &str,
    retention_priority: i64,
    title: &str,
    summary: Option<&str>,
    body: &str,
    source_ref: &str,
    created_at: &str,
) -> MemoryItem {
    MemoryItem {
        row_id: 0,
        id: new_uuid_v4(),
        project_id: Some(project_id.to_owned()),
        task_id: None,
        execution_id: None,
        scope_type: "project".to_owned(),
        scope_id: project_id.to_owned(),
        visibility: "project".to_owned(),
        owner_identity_id: None,
        authority: authority.to_owned(),
        sensitivity: "internal".to_owned(),
        retention_priority,
        provenance_json: "{}".to_owned(),
        publication_source_id: None,
        supersedes_id: None,
        valid_from: Some(created_at.to_owned()),
        valid_until: None,
        source_event_id: None,
        source_scope_type: Some("project".to_owned()),
        source_scope_id: Some(project_id.to_owned()),
        source_revision: Some("1".to_owned()),
        source_type: "test".to_owned(),
        kind: "observation".to_owned(),
        title: title.to_owned(),
        summary: summary.map(str::to_owned),
        body: body.to_owned(),
        metadata_json: json!({"source_ref": source_ref}).to_string(),
        confidence: Some("confirmed".to_owned()),
        quality_score: None,
        created_by_type: Some("test".to_owned()),
        created_by_id: None,
        created_at: created_at.to_owned(),
    }
}
