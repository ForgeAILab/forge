use std::sync::Arc;

use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentContextScopeRepo, AgentRepo,
    AgentStatus, CreateAgentContextScope, CreateAgentIdentity, CreateAgentProfile, CreateProject,
    MemoryItem, MemoryRepository, MemoryScopeGrant, ProjectRepo, ScopedMemoryRepository, SqliteDb,
};
use serde_json::json;
use services::{
    ContextManifestInput, ContextManifestService, ContextSourceInput, ForgeMemoryQuery,
    ForgeMemorySource, MemoryAccessContext, MemoryLifecycleInput, MemoryPublicationInput,
    MemoryService, MemorySourceBindingInput,
};
use uuid::Uuid;

#[tokio::test]
async fn memory_source_authority_is_immutable_and_text_cannot_expand_scope() {
    let db = Arc::new(sqlite_db().await);
    let now = now_rfc3339();
    let project_a = new_uuid_v4();
    let project_b = new_uuid_v4();
    ProjectRepo::create(
        db.as_ref(),
        CreateProject {
            id: project_a.clone(),
            name: "memory authority A".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project A creates");
    ProjectRepo::create(
        db.as_ref(),
        CreateProject {
            id: project_b.clone(),
            name: "memory authority B".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project B creates");

    let identity_id = new_uuid_v4();
    let profile_id = new_uuid_v4();
    AgentRepo::create_identity_with_profile(
        db.as_ref(),
        CreateAgentIdentity {
            id: identity_id.clone(),
            name: "memory-authority-agent".to_owned(),
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
            id: profile_id,
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
            scope_id: project_a.clone(),
            project_id: Some(project_a.clone()),
            task_id: None,
            task_role: None,
            workspace_access: "deny".to_owned(),
            workspace_path: None,
            authority_json: json!({
                "issued_to_user_id": "not-a-text-field",
                "scope": { "type": "project", "project_id": project_a }
            })
            .to_string(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("context scope creates");

    let allowed = memory_item(
        &project_a,
        &project_a,
        "shared-needle project-b claim in project-a text",
        "internal",
    );
    let other_project = memory_item(
        &project_b,
        &project_b,
        "shared-needle project-b body",
        "internal",
    );
    let restricted = memory_item(
        &project_a,
        &project_a,
        "shared-needle restricted body",
        "restricted",
    );
    let secret = memory_item(
        &project_a,
        &project_a,
        "shared-needle secret body",
        "secret",
    );
    for item in [&allowed, &other_project, &restricted, &secret] {
        MemoryRepository::insert_memory_item(db.as_ref(), item)
            .await
            .expect("memory item inserts");
    }

    let source = ForgeMemorySource::bind(
        Arc::clone(&db),
        MemorySourceBindingInput {
            binding_id: Uuid::new_v4(),
            identity_id: Uuid::parse_str(&identity_id).expect("identity is UUID"),
            context_scope_id: Uuid::parse_str(&context_scope_id).expect("scope is UUID"),
            scope_type: "project".to_owned(),
            scope_id: project_a.clone(),
            account_id: None,
            project_id: Some(project_a.clone()),
            task_id: None,
            policy_revision: "policy-1".to_owned(),
            access: MemoryAccessContext {
                identity_id: Some(identity_id.clone()),
                grants: vec![
                    MemoryScopeGrant {
                        scope_type: "project".to_owned(),
                        scope_id: project_a.clone(),
                        visibility: vec!["project".to_owned()],
                        identity_id: None,
                    },
                    MemoryScopeGrant {
                        scope_type: "project".to_owned(),
                        scope_id: project_b.clone(),
                        visibility: vec!["project".to_owned()],
                        identity_id: None,
                    },
                ],
            },
        },
    )
    .await
    .expect("source binds to admitted project scope");
    let persisted_room_id = sqlx::query_scalar::<_, Option<String>>(
        "SELECT room_id FROM forge_memory_source_binding WHERE id = ?",
    )
    .bind(source.binding_id())
    .fetch_one(db.pool())
    .await
    .expect("source binding loads");
    assert!(
        persisted_room_id.is_none(),
        "live bindings never populate Room provenance"
    );

    let results = source
        .search(ForgeMemoryQuery {
            // The query contains another project id as ordinary text. It is
            // still only an FTS query; the immutable grant remains project A.
            query: "shared-needle project-b".to_owned(),
            limit: 50,
            represented_source_ids: Vec::new(),
            cursor: None,
        })
        .await
        .expect("scoped search succeeds");
    assert_eq!(results.records.len(), 1);
    assert_eq!(results.records[0].id, Uuid::parse_str(&allowed.id).unwrap());
    assert_eq!(results.records[0].body, allowed.body);
    assert!(!results
        .records
        .iter()
        .any(|record| record.id == Uuid::parse_str(&other_project.id).unwrap()));
    assert!(!results
        .records
        .iter()
        .any(|record| record.id == Uuid::parse_str(&restricted.id).unwrap()));
    assert!(!results
        .records
        .iter()
        .any(|record| record.id == Uuid::parse_str(&secret.id).unwrap()));

    let room_scope = ForgeMemorySource::bind(
        Arc::clone(&db),
        MemorySourceBindingInput {
            binding_id: Uuid::new_v4(),
            identity_id: Uuid::parse_str(&identity_id).unwrap(),
            context_scope_id: Uuid::parse_str(&context_scope_id).unwrap(),
            scope_type: "room".to_owned(),
            scope_id: "legacy-room".to_owned(),
            account_id: None,
            project_id: None,
            task_id: None,
            policy_revision: "policy-room".to_owned(),
            access: MemoryAccessContext::for_scope(
                Some(identity_id.clone()),
                "room",
                "legacy-room",
                vec!["participants".to_owned()],
            ),
        },
    )
    .await;
    assert!(
        room_scope.is_err(),
        "live memory sources cannot bind legacy Rooms"
    );

    let mismatch = ForgeMemorySource::bind(
        Arc::clone(&db),
        MemorySourceBindingInput {
            binding_id: Uuid::new_v4(),
            identity_id: Uuid::parse_str(&identity_id).unwrap(),
            context_scope_id: Uuid::parse_str(&context_scope_id).unwrap(),
            scope_type: "project".to_owned(),
            scope_id: project_b,
            account_id: None,
            project_id: None,
            task_id: None,
            policy_revision: "policy-2".to_owned(),
            access: MemoryAccessContext::for_scope(
                Some(identity_id),
                "project",
                project_a,
                vec!["project".to_owned()],
            ),
        },
    )
    .await;
    assert!(mismatch.is_err(), "binding must reject a mismatched grant");
}

#[tokio::test]
async fn context_manifest_rejects_secret_source_even_when_text_claims_public_scope() {
    let db = Arc::new(sqlite_db().await);
    let identity_id = Uuid::new_v4();
    let context_scope_id = Uuid::new_v4();
    let manifest = services::ContextManifestService::new(Arc::clone(&db));
    let error = manifest
        .create(
            services::ContextManifestInput {
                id: Uuid::new_v4(),
                identity_id,
                agent_session_id: None,
                context_scope_id,
                scope_type: "project".to_owned(),
                scope_id: "text-does-not-authorize-project".to_owned(),
                policy_revision: "policy".to_owned(),
                domain_revision: "domain".to_owned(),
                lcm_binding_revision: None,
                runtime_manifest_id: None,
                runtime_manifest_fingerprint: None,
                request_fingerprint: "request".to_owned(),
            },
            &[services::ContextSourceInput {
                ordinal: 0,
                source_id: "memory-secret".to_owned(),
                source_type: "room_message".to_owned(),
                source_revision: "1".to_owned(),
                selection_reason: "the body claims it is public".to_owned(),
                disposition: "included".to_owned(),
                retention_priority: 0,
                fragment_fingerprint: "fingerprint".to_owned(),
                sensitivity: "secret".to_owned(),
            }],
        )
        .await
        .expect_err("secret source must never enter a context manifest");
    assert!(error.to_string().contains("secret content"));

    let reason_error = manifest
        .create(
            services::ContextManifestInput {
                id: Uuid::new_v4(),
                identity_id: Uuid::new_v4(),
                agent_session_id: None,
                context_scope_id: Uuid::new_v4(),
                scope_type: "project".to_owned(),
                scope_id: "project-id".to_owned(),
                policy_revision: "policy".to_owned(),
                domain_revision: "domain".to_owned(),
                lcm_binding_revision: None,
                runtime_manifest_id: None,
                runtime_manifest_fingerprint: None,
                request_fingerprint: "request".to_owned(),
            },
            &[services::ContextSourceInput {
                ordinal: 0,
                source_id: "memory-safe-id".to_owned(),
                source_type: "room_message".to_owned(),
                source_revision: "1".to_owned(),
                selection_reason: "Authorization: Bearer sk-secret".to_owned(),
                disposition: "included".to_owned(),
                retention_priority: 0,
                fragment_fingerprint: "fingerprint".to_owned(),
                sensitivity: "internal".to_owned(),
            }],
        )
        .await
        .expect_err("protected selection rationale must not be persisted");
    assert!(reason_error.to_string().contains("protected values"));
}

#[tokio::test]
async fn context_manifest_sources_require_the_manifest_scope_before_retrieval() {
    let db = Arc::new(sqlite_db().await);
    let owner_id = seed_identity(db.as_ref(), "manifest-owner").await;
    let outsider_id = seed_identity(db.as_ref(), "manifest-outsider").await;
    let context_scope_id = Uuid::new_v4();
    let now = now_rfc3339();
    AgentContextScopeRepo::create_context_scope(
        db.as_ref(),
        CreateAgentContextScope {
            id: context_scope_id.to_string(),
            identity_id: owner_id.clone(),
            scope_type: "project".to_owned(),
            scope_id: "project-boundary".to_owned(),
            project_id: None,
            task_id: None,
            task_role: None,
            workspace_access: "deny".to_owned(),
            workspace_path: None,
            authority_json: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("context scope creates");

    let service = ContextManifestService::new(Arc::clone(&db));
    let manifest_id = Uuid::new_v4();
    let manifest = service
        .create(
            ContextManifestInput {
                id: manifest_id,
                identity_id: Uuid::parse_str(&owner_id).expect("owner UUID"),
                agent_session_id: None,
                context_scope_id,
                scope_type: "project".to_owned(),
                scope_id: "project-boundary".to_owned(),
                policy_revision: "policy".to_owned(),
                domain_revision: "domain".to_owned(),
                lcm_binding_revision: None,
                runtime_manifest_id: None,
                runtime_manifest_fingerprint: None,
                request_fingerprint: "request".to_owned(),
            },
            &[],
        )
        .await
        .expect("manifest creates");
    let owner_uuid = Uuid::parse_str(&owner_id).expect("owner UUID");
    let outsider_uuid = Uuid::parse_str(&outsider_id).expect("outsider UUID");
    let source = ContextSourceInput {
        ordinal: 0,
        source_id: "project-document-1".to_owned(),
        source_type: "project_document".to_owned(),
        source_revision: "revision-1".to_owned(),
        selection_reason: "approved".to_owned(),
        disposition: "included".to_owned(),
        retention_priority: 100,
        fragment_fingerprint: "digest".to_owned(),
        sensitivity: "internal".to_owned(),
    };
    let denied = service
        .append_source(manifest_id, outsider_uuid, context_scope_id, source.clone())
        .await;
    assert!(denied.is_err(), "foreign identity cannot append a source");
    assert!(
        service
            .sources(manifest_id, outsider_uuid, context_scope_id)
            .await
            .is_err(),
        "foreign identity cannot observe source existence"
    );
    service
        .append_source(manifest_id, owner_uuid, context_scope_id, source)
        .await
        .expect("owner appends source");
    assert_eq!(
        service
            .sources(manifest_id, owner_uuid, context_scope_id)
            .await
            .expect("owner reads sources")
            .len(),
        1
    );
    assert_eq!(manifest.id, manifest_id.to_string());
}

#[tokio::test]
async fn publication_is_immutable_and_destructive_lifecycle_is_owner_only() {
    let db = Arc::new(sqlite_db().await);
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    ProjectRepo::create(
        db.as_ref(),
        CreateProject {
            id: project_id.clone(),
            name: "publication immutability".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("project creates");
    let identity_id = seed_identity(db.as_ref(), "publisher").await;
    let mut source = memory_item(
        &project_id,
        &project_id,
        "private assertion body",
        "internal",
    );
    source.visibility = "private".to_owned();
    source.owner_identity_id = Some(identity_id.clone());
    MemoryRepository::insert_memory_item(db.as_ref(), &source)
        .await
        .expect("private memory inserts");

    let access = MemoryAccessContext::for_scope(
        Some(identity_id.clone()),
        "project",
        project_id.clone(),
        vec!["project".to_owned()],
    );
    let service = MemoryService::new(Arc::clone(&db));
    let published = service
        .publish(
            &access,
            MemoryPublicationInput {
                source_id: Uuid::parse_str(&source.id).expect("source id is UUID"),
                source_scope_type: "project".to_owned(),
                source_scope_id: project_id.clone(),
                target_scope_type: "project".to_owned(),
                target_scope_id: project_id.clone(),
                target_project_id: Some(project_id.clone()),
                target_task_id: None,
                target_visibility: "project".to_owned(),
                target_authority: "observation".to_owned(),
                actor_identity_id: identity_id.clone(),
                reason: "explicit publication".to_owned(),
                evidence_json: "{}".to_owned(),
            },
        )
        .await
        .expect("publication succeeds");
    assert_ne!(published.id, source.id);
    assert_eq!(published.visibility, "project");
    assert_eq!(published.source_scope_type.as_deref(), Some("project"));
    assert_eq!(
        published.source_scope_id.as_deref(),
        Some(project_id.as_str())
    );
    assert_eq!(
        published.source_revision.as_deref(),
        source.source_revision.as_deref(),
        "publication must retain the canonical source revision"
    );
    let mut missing_revision = source.clone();
    missing_revision.id = new_uuid_v4();
    missing_revision.source_revision = None;
    MemoryRepository::insert_memory_item(db.as_ref(), &missing_revision)
        .await
        .expect("missing-revision source inserts for rejection test");
    let missing_revision_publication = service
        .publish(
            &access,
            MemoryPublicationInput {
                source_id: Uuid::parse_str(&missing_revision.id).expect("source id is UUID"),
                source_scope_type: "project".to_owned(),
                source_scope_id: project_id.clone(),
                target_scope_type: "project".to_owned(),
                target_scope_id: project_id.clone(),
                target_project_id: Some(project_id.clone()),
                target_task_id: None,
                target_visibility: "project".to_owned(),
                target_authority: "observation".to_owned(),
                actor_identity_id: identity_id.clone(),
                reason: "missing revision must fail closed".to_owned(),
                evidence_json: "{}".to_owned(),
            },
        )
        .await;
    assert!(
        missing_revision_publication.is_err(),
        "memory without a canonical source revision cannot be published"
    );
    let source_after = MemoryRepository::get_memory_item(db.as_ref(), &source.id)
        .await
        .expect("source loads")
        .expect("source remains");
    assert_eq!(source_after.visibility, "private");
    assert_eq!(source_after.body, source.body);
    let publication_audit =
        ScopedMemoryRepository::list_memory_lifecycle_assertions(db.as_ref(), &source.id)
            .await
            .expect("publication audit loads");
    assert_eq!(publication_audit.len(), 1);
    assert_eq!(publication_audit[0].assertion_type, "published");
    assert_eq!(
        publication_audit[0].related_memory_id.as_deref(),
        Some(published.id.as_str())
    );

    let outsider = MemoryAccessContext::for_scope(
        Some("outsider".to_owned()),
        "project",
        project_id.clone(),
        vec!["project".to_owned()],
    );
    let denied = service
        .assert_lifecycle(
            &outsider,
            MemoryLifecycleInput {
                memory_id: Uuid::parse_str(&published.id).expect("published id is UUID"),
                assertion_type: "retracted".to_owned(),
                related_memory_id: None,
                reason: Some("not owner".to_owned()),
                evidence_json: "{}".to_owned(),
                actor_identity_id: "outsider".to_owned(),
            },
        )
        .await;
    assert!(denied.is_err(), "shared access cannot retract a record");

    service
        .assert_lifecycle(
            &access,
            MemoryLifecycleInput {
                memory_id: Uuid::parse_str(&published.id).expect("published id is UUID"),
                assertion_type: "retracted".to_owned(),
                related_memory_id: None,
                reason: Some("owner retraction".to_owned()),
                evidence_json: "{}".to_owned(),
                actor_identity_id: identity_id,
            },
        )
        .await
        .expect("owner retraction succeeds");
    assert!(service
        .get_scoped(
            &access,
            Uuid::parse_str(&published.id).expect("published id is UUID"),
            None,
        )
        .await
        .is_err());
}

async fn sqlite_db() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    SqliteDb::new(pool)
}

async fn seed_identity(db: &SqliteDb, name: &str) -> String {
    let identity_id = new_uuid_v4();
    let now = now_rfc3339();
    AgentRepo::create_identity_with_profile(
        db,
        CreateAgentIdentity {
            id: identity_id.clone(),
            name: name.to_owned(),
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
            updated_at: now,
        },
    )
    .await
    .expect("identity creates");
    identity_id
}

fn memory_item(project_id: &str, scope_id: &str, body: &str, sensitivity: &str) -> MemoryItem {
    let now = now_rfc3339();
    MemoryItem {
        row_id: 0,
        id: new_uuid_v4(),
        project_id: Some(project_id.to_owned()),
        task_id: None,
        execution_id: None,
        scope_type: "project".to_owned(),
        scope_id: scope_id.to_owned(),
        visibility: "project".to_owned(),
        owner_identity_id: None,
        authority: "observation".to_owned(),
        sensitivity: sensitivity.to_owned(),
        retention_priority: 10,
        provenance_json: "{}".to_owned(),
        publication_source_id: None,
        supersedes_id: None,
        valid_from: Some(now.clone()),
        valid_until: None,
        source_event_id: None,
        source_scope_type: Some("project".to_owned()),
        source_scope_id: Some(scope_id.to_owned()),
        source_revision: Some("1".to_owned()),
        source_type: "test".to_owned(),
        kind: "observation".to_owned(),
        title: "adversarial memory".to_owned(),
        summary: None,
        body: body.to_owned(),
        metadata_json: json!({"source_ref": body}).to_string(),
        confidence: Some("confirmed".to_owned()),
        quality_score: Some(1),
        created_by_type: Some("test".to_owned()),
        created_by_id: None,
        created_at: now,
    }
}
