//! V090 regression coverage for Project teardown.
//!
//! `context_manifest` and `context_manifest_source` blocked deletion
//! unconditionally, and they are reached by cascade from `DELETE FROM project`.
//! Any Project whose agent had produced a single context manifest could
//! therefore never be deleted. The triggers must still refuse a direct delete
//! while the parent scope exists.

use db::{
    create_sqlite_pool, new_uuid_v4, run_migrations, AgentContextScopeRepo, AgentRepo,
    CreateAgentContextScope, CreateAgentIdentity, CreateAgentProfile, CreateContextManifest,
    CreateContextManifestSource, CreateProject, ProjectRepo, ScopedMemoryRepository, SqliteDb,
    User, UserRepo,
};

const NOW: &str = "2026-08-21T00:00:00.000Z";
const ACCOUNT_ID: &str = "manifest-teardown-account";

async fn database() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    SqliteDb::new(pool)
}

/// Build a Project that owns one context manifest with one source, exactly the
/// shape a Project acquires as soon as its agent takes a single turn.
async fn project_with_context_manifest(db: &SqliteDb) -> (String, String) {
    UserRepo::create_user(
        db,
        &User {
            id: ACCOUNT_ID.to_owned(),
            email: "manifest-teardown@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("account creates");

    let project = ProjectRepo::create(
        db,
        CreateProject {
            id: new_uuid_v4(),
            name: "manifest teardown".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(ACCOUNT_ID.to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("project creates");

    let identity_id = new_uuid_v4();
    AgentRepo::create_identity_with_profile(
        db,
        CreateAgentIdentity {
            id: identity_id.clone(),
            name: "manifest teardown agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: db::AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some(ACCOUNT_ID.to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
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
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("identity creates");

    let scope = AgentContextScopeRepo::create_context_scope(
        db,
        CreateAgentContextScope {
            id: new_uuid_v4(),
            identity_id: identity_id.clone(),
            scope_type: "project".to_owned(),
            scope_id: project.id.clone(),
            project_id: Some(project.id.clone()),
            task_id: None,
            task_role: None,
            workspace_access: "deny".to_owned(),
            authority_json: "{}".to_owned(),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("context scope creates");

    let manifest = ScopedMemoryRepository::create_context_manifest(
        db,
        CreateContextManifest {
            id: new_uuid_v4(),
            identity_id,
            agent_session_id: None,
            context_scope_id: scope.id.clone(),
            scope_type: "project".to_owned(),
            scope_id: project.id.clone(),
            policy_revision: "policy-1".to_owned(),
            domain_revision: "domain-1".to_owned(),
            lcm_binding_revision: None,
            runtime_manifest_id: None,
            runtime_manifest_fingerprint: None,
            combined_fingerprint: "combined-fingerprint".to_owned(),
            request_fingerprint: "request-fingerprint".to_owned(),
            created_at: NOW.to_owned(),
        },
    )
    .await
    .expect("context manifest creates");

    ScopedMemoryRepository::append_context_manifest_source(
        db,
        CreateContextManifestSource {
            manifest_id: manifest.id.clone(),
            ordinal: 0,
            source_id: new_uuid_v4(),
            source_type: "memory_item".to_owned(),
            source_revision: NOW.to_owned(),
            selection_reason: "same canonical Project scope".to_owned(),
            disposition: "included".to_owned(),
            retention_priority: 10,
            fragment_fingerprint: "fragment-fingerprint".to_owned(),
        },
    )
    .await
    .expect("manifest source appends");

    (project.id, manifest.id)
}

#[tokio::test]
async fn a_project_that_produced_a_context_manifest_can_be_deleted() {
    let db = database().await;
    let (project_id, manifest_id) = project_with_context_manifest(&db).await;

    ProjectRepo::delete(&db, &project_id)
        .await
        .expect("Project teardown cascades through its context manifests");

    for (query, label) in [
        ("SELECT COUNT(*) FROM project WHERE id = ?", "project"),
        (
            "SELECT COUNT(*) FROM agent_context_scope WHERE project_id = ?",
            "context scope",
        ),
    ] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>(query)
                .bind(&project_id)
                .fetch_one(db.pool())
                .await
                .unwrap_or_else(|error| panic!("{label} count: {error}")),
            0,
            "{label} rows are gone after teardown"
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM context_manifest WHERE id = ?")
            .bind(&manifest_id)
            .fetch_one(db.pool())
            .await
            .expect("manifest count"),
        0,
        "the cascade removed the manifest with its scope"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM context_manifest_source WHERE manifest_id = ?"
        )
        .bind(&manifest_id)
        .fetch_one(db.pool())
        .await
        .expect("manifest source count"),
        0,
        "the cascade removed the manifest's sources"
    );
}

#[tokio::test]
async fn a_live_context_manifest_still_refuses_direct_mutation() {
    let db = database().await;
    let (_project_id, manifest_id) = project_with_context_manifest(&db).await;

    // The relaxed trigger must still protect a manifest whose scope is live:
    // immutability means "cannot be rewritten or removed under a scope that
    // still exists", not "the owning Project is permanent".
    let deleted = sqlx::query("DELETE FROM context_manifest WHERE id = ?")
        .bind(&manifest_id)
        .execute(db.pool())
        .await;
    assert!(
        deleted.is_err(),
        "a manifest under a live scope cannot be deleted directly"
    );

    let deleted_source = sqlx::query("DELETE FROM context_manifest_source WHERE manifest_id = ?")
        .bind(&manifest_id)
        .execute(db.pool())
        .await;
    assert!(
        deleted_source.is_err(),
        "a manifest source under a live manifest cannot be deleted directly"
    );

    let updated = sqlx::query("UPDATE context_manifest SET scope_id = 'rewritten' WHERE id = ?")
        .bind(&manifest_id)
        .execute(db.pool())
        .await;
    assert!(
        updated.is_err(),
        "manifest updates stay unconditionally immutable"
    );

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM context_manifest WHERE id = ?")
            .bind(&manifest_id)
            .fetch_one(db.pool())
            .await
            .expect("manifest count"),
        1,
        "the manifest survived every refused mutation"
    );
}
