use std::sync::Arc;

use db::{
    create_sqlite_pool, run_migrations, AgentRepo, AgentStatus, CreateAgentIdentity,
    CreateAgentProfile, SqliteDb,
};
use forge_agent_host::{
    CanonicalScope, CanonicalScopeType, WorkspaceAccess, MAIN_CHARTER_APPROVAL_TARGET_OPERATION,
    MAIN_CHARTER_DIFF_OPERATION, MAIN_CHARTER_READINESS_OPERATION, MAIN_CHARTER_READ_OPERATION,
};
use serde_json::{json, Value};
use services::MainOrchestrationQueryService;

const ACCOUNT_ID: &str = "query-account";
const MAIN_IDENTITY_ID: &str = "query-main-agent";
const UNBOUND_IDENTITY_ID: &str = "query-unbound-agent";
const MAIN_CHAT_ID: &str = "query-main-chat";
const GENESIS_ID: &str = "query-genesis";
const CHARTER_ID: &str = "query-charter";
const REVISION_ONE_ID: &str = "query-charter-r1";
const REVISION_TWO_ID: &str = "query-charter-r2";

async fn fixture() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    let db = Arc::new(SqliteDb::new(pool));
    let now = db::now_rfc3339();
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES (?, ?, 'test', 'Query User', ?, ?)",
    )
    .bind(ACCOUNT_ID)
    .bind("query@example.test")
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("user");

    let permissions = r#"{"permissions":["read_account","read_agent_chat","propose_discovery"]}"#;
    insert_identity(&db, MAIN_IDENTITY_ID, permissions).await;
    insert_identity(&db, UNBOUND_IDENTITY_ID, permissions).await;

    sqlx::query(
        "UPDATE agent_chat SET id = ?, status = 'ready'
         WHERE account_id = ? AND kind = 'account_main'",
    )
    .bind(MAIN_CHAT_ID)
    .bind(ACCOUNT_ID)
    .execute(db.pool())
    .await
    .expect("Main Chat");
    let profile_id: String =
        sqlx::query_scalar("SELECT selected_profile_id FROM agent_identity WHERE id = ?")
            .bind(MAIN_IDENTITY_ID)
            .fetch_one(db.pool())
            .await
            .expect("Main profile");
    sqlx::query(
        "INSERT INTO account_main_agent_binding
         (id, account_id, identity_id, profile_id, state, autonomy_policy_json,
          tool_policy_revision, version, created_at, updated_at)
         VALUES ('query-binding', ?, ?, ?, 'active', '{}', 'test', 1, ?, ?)",
    )
    .bind(ACCOUNT_ID)
    .bind(MAIN_IDENTITY_ID)
    .bind(profile_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("Main binding");

    sqlx::query(
        "INSERT INTO product_genesis_session
         (id, account_id, main_chat_id, prompt_revision, prompt_body, maturity,
          lifecycle, source_message_ids_json, version, created_at, updated_at)
         VALUES (?, ?, ?, 'query-prompt', 'Query fixture', 'mvp', 'discovering', '[]', 1, ?, ?)",
    )
    .bind(GENESIS_ID)
    .bind(ACCOUNT_ID)
    .bind(MAIN_CHAT_ID)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("Genesis");

    let content_one = charter_content("Query Charter");
    let content_two = charter_content("Query Charter Revised");
    sqlx::query(
        "INSERT INTO project_charter
         (id, account_id, genesis_session_id, project_mode, maturity, lifecycle,
          version, created_at, updated_at)
         VALUES (?, ?, ?, 'compact', 'mvp', 'ready_for_approval', 1, ?, ?)",
    )
    .bind(CHARTER_ID)
    .bind(ACCOUNT_ID)
    .bind(GENESIS_ID)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("Charter");
    insert_revision(&db, REVISION_ONE_ID, 1, 0, None, &content_one, &now).await;
    insert_revision(
        &db,
        REVISION_TWO_ID,
        2,
        1,
        Some(REVISION_ONE_ID),
        &content_two,
        &now,
    )
    .await;
    sqlx::query(
        "UPDATE project_charter
         SET current_draft_revision_id = ?, current_approved_revision_id = NULL
         WHERE id = ?",
    )
    .bind(REVISION_TWO_ID)
    .bind(CHARTER_ID)
    .execute(db.pool())
    .await
    .expect("Charter pointers");
    sqlx::query(
        "UPDATE product_genesis_session
         SET charter_id = ?, charter_revision_id = ?, charter_version = 1
         WHERE id = ?",
    )
    .bind(CHARTER_ID)
    .bind(REVISION_TWO_ID)
    .bind(GENESIS_ID)
    .execute(db.pool())
    .await
    .expect("Genesis Charter pointer");
    db
}

async fn insert_identity(db: &SqliteDb, identity_id: &str, permissions: &str) {
    let now = db::now_rfc3339();
    let profile_id = format!("{identity_id}-profile");
    AgentRepo::create_identity_with_profile(
        db,
        CreateAgentIdentity {
            id: identity_id.to_owned(),
            name: identity_id.to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some(ACCOUNT_ID.to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: permissions.to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: profile_id,
            identity_id: identity_id.to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "native".to_owned(),
            provider: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: permissions.to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("identity");
}

fn charter_content(name: &str) -> Value {
    json!({
        "identity": {
            "working_name": name,
            "slug_proposal": "query-charter",
            "one_line_vision": "Read-only Charter projections",
            "maturity": "mvp"
        },
        "problem_and_people": {
            "problem_or_opportunity": "Query state must be bounded",
            "target_users": ["maintainers"]
        },
        "core_experience": {"primary_outcome": "Bounded reads"},
        "scope": {
            "must_have_outcomes": ["No action rows"],
            "explicit_non_goals": ["Mutation"]
        },
        "success": {"acceptance_statements": ["Queries remain read-only"]},
        "constraints_and_risks": {},
        "knowledge_ledger": {"items": []}
    })
}

async fn insert_revision(
    db: &SqliteDb,
    id: &str,
    revision: i64,
    base_revision: i64,
    base_revision_id: Option<&str>,
    content: &Value,
    now: &str,
) {
    sqlx::query(
        "INSERT INTO project_charter_revision
         (id, charter_id, revision, base_revision, base_revision_id, lifecycle,
          schema_version, render_version, content_json, rendered_view, change_summary,
          author_type, author_id, source_refs_json, content_digest, rendered_digest, created_at)
         VALUES (?, ?, ?, ?, ?, 'proposed', 'charter-v1', 'render-v1', ?, ?,
                 'query fixture', 'agent', ?, '[]', ?, ?, ?)",
    )
    .bind(id)
    .bind(CHARTER_ID)
    .bind(revision)
    .bind(base_revision)
    .bind(base_revision_id)
    .bind(content.to_string())
    .bind(format!("rendered-{revision}"))
    .bind(MAIN_IDENTITY_ID)
    .bind(format!("content-{revision}"))
    .bind(format!("render-{revision}"))
    .bind(now)
    .execute(db.pool())
    .await
    .expect("Charter revision");
}

fn main_scope() -> CanonicalScope {
    CanonicalScope {
        scope_type: CanonicalScopeType::Account,
        scope_id: ACCOUNT_ID.to_owned(),
        workspace_access: WorkspaceAccess::Deny,
    }
}

fn projection_arguments() -> Value {
    json!({
        "genesis_session_id": GENESIS_ID,
        "charter_id": CHARTER_ID,
        "revision_id": REVISION_TWO_ID,
        "content_digest": "content-2",
        "render_digest": "render-2",
        "expected_charter_version": 1
    })
}

#[tokio::test]
async fn main_charter_queries_do_not_create_action_or_receipt_rows() {
    let db = fixture().await;
    let service = MainOrchestrationQueryService::new(Arc::clone(&db));
    let scope = main_scope();
    let before = counts(&db).await;

    let read = service
        .execute(
            MAIN_IDENTITY_ID,
            &scope,
            MAIN_CHARTER_READ_OPERATION,
            json!({
                "genesis_session_id": GENESIS_ID,
                "charter_id": CHARTER_ID,
                "revision_id": REVISION_TWO_ID
            }),
        )
        .await
        .expect("charter read query");
    assert_eq!(read["scope"], "main");
    assert_eq!(read["items"][0]["id"], CHARTER_ID);

    let readiness = service
        .execute(
            MAIN_IDENTITY_ID,
            &scope,
            MAIN_CHARTER_READINESS_OPERATION,
            projection_arguments(),
        )
        .await
        .expect("readiness query");
    assert_eq!(readiness["operation"], MAIN_CHARTER_READINESS_OPERATION);

    let diff = service
        .execute(
            MAIN_IDENTITY_ID,
            &scope,
            MAIN_CHARTER_DIFF_OPERATION,
            json!({
                "genesis_session_id": GENESIS_ID,
                "charter_id": CHARTER_ID,
                "base_revision_id": REVISION_ONE_ID,
                "candidate_revision_id": REVISION_TWO_ID
            }),
        )
        .await
        .expect("diff query");
    assert_eq!(diff["operation"], MAIN_CHARTER_DIFF_OPERATION);

    let target = service
        .execute(
            MAIN_IDENTITY_ID,
            &scope,
            MAIN_CHARTER_APPROVAL_TARGET_OPERATION,
            projection_arguments(),
        )
        .await
        .expect("approval target query");
    assert_eq!(target["operation"], MAIN_CHARTER_APPROVAL_TARGET_OPERATION);
    assert_eq!(counts(&db).await, before);
}

#[tokio::test]
async fn main_charter_queries_reject_cross_scope_and_unbound_identities() {
    let db = fixture().await;
    let service = MainOrchestrationQueryService::new(Arc::clone(&db));
    let mut wrong_scope = main_scope();
    wrong_scope.scope_id = "other-account".to_owned();
    assert!(service
        .execute(
            MAIN_IDENTITY_ID,
            &wrong_scope,
            MAIN_CHARTER_READINESS_OPERATION,
            projection_arguments(),
        )
        .await
        .is_err());
    assert!(service
        .execute(
            UNBOUND_IDENTITY_ID,
            &main_scope(),
            MAIN_CHARTER_APPROVAL_TARGET_OPERATION,
            projection_arguments(),
        )
        .await
        .is_err());
}

async fn counts(db: &SqliteDb) -> (i64, i64, i64) {
    (
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_action")
            .fetch_one(db.pool())
            .await
            .expect("actions count"),
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_action_execution")
            .fetch_one(db.pool())
            .await
            .expect("executions count"),
        sqlx::query_scalar("SELECT COUNT(*) FROM command_receipt")
            .fetch_one(db.pool())
            .await
            .expect("receipts count"),
    )
}
