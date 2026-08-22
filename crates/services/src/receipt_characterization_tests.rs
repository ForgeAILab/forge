//! Characterization coverage for the pre-Gate-A action receipt seam.
//!
//! These tests deliberately exercise the current two-transaction path with a
//! one-shot test-only stop between the domain commit and
//! `agent_action_execution`.  The assertions document the stranded/duplicate
//! outcomes that Gate A must remove.  Once the shared command boundary lands,
//! the same scenarios should assert rollback or exact replay instead.

use std::sync::Arc;

use api_types::{
    AdaptiveEnvelope, ArtifactRef, ExecutionBaselineContent, ExecutionBaselineReleasePolicy,
    PrincipalKind, PrincipalRef, ProductMaturity, ProjectCharterContent, ProjectMode,
    ProvenanceRef, ProvenanceSourceKind, RevisionProvenance,
};
use db::{
    now_rfc3339, run_migrations, AgentAction, AgentActionPolicyResult, AgentActionRepo,
    AgentActionStatus, AgentRepo, AgentStatus, CreateAgentAction, CreateAgentIdentity,
    CreateAgentProfile, CreateProject, ProjectRepo, SqliteDb,
};
use events::EventBus;
use forge_agent_host::{
    CanonicalScope, CanonicalScopeType, WorkspaceAccess, MAIN_CHARTER_DRAFT_OPERATION,
    PROJECT_DOCUMENT_OPERATION, PROJECT_EXECUTION_BASELINE_OPERATION,
};
use serde_json::{json, Value};

use crate::{
    test_support::arm_after_domain_commit, AgentActionService,
    ExecuteProjectOrchestrationActionInput, ExecuteTaskProposalInput,
    MainGenesisCharterDraftRequest, MainGenesisCommandService, MainGenesisDraftCommandInput,
    MainGenesisDraftPrincipal, ProjectArtifactCommandService, ProjectCommandAuthorization,
    ProjectDocumentCreateCommand, ProjectOrchestrationActionService, ServiceError, TaskService,
};

const USER_ID: &str = "characterization-user";

fn document_create_authorization(event_id: &str) -> ProjectCommandAuthorization {
    ProjectCommandAuthorization {
        principal_type: "user".to_owned(),
        principal_id: USER_ID.to_owned(),
        policy_result: "allowed".to_owned(),
        policy_revision: None,
        policy_digest: None,
        requested_permission: Some("project.document.create".to_owned()),
        correlation_id: event_id.to_owned(),
        causation_id: None,
        causation_depth: 0,
        authorization_event_id: event_id.to_owned(),
        authorization_basis: "explicit_user_document_create".to_owned(),
        authorization_action: "project.document.create".to_owned(),
        authorization_occurred_at: "2026-08-20T00:00:00.000Z".to_owned(),
        authorization_json: json!({
            "principal": {"kind": "user", "id": USER_ID},
            "action": "project.document.create",
            "event_id": event_id,
        })
        .to_string(),
    }
}

struct ProjectFixture {
    db: Arc<SqliteDb>,
    project_id: String,
    agent_id: String,
}

async fn database() -> Arc<SqliteDb> {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("in-memory SQLite pool");
    run_migrations(&pool).await.expect("schema migrations");
    Arc::new(SqliteDb::new(pool))
}

async fn insert_user(db: &SqliteDb) {
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES (?, ?, 'test', 'Characterization User', ?, ?)",
    )
    .bind(USER_ID)
    .bind(format!("{USER_ID}@example.test"))
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("characterization user");
}

async fn insert_identity(db: &SqliteDb, identity_id: &str, permissions: &str) -> String {
    let now = now_rfc3339();
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
            owner_id: Some(USER_ID.to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: permissions.to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: profile_id.clone(),
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
    .expect("characterization identity");
    profile_id
}

async fn project_fixture() -> ProjectFixture {
    let db = database().await;
    insert_user(&db).await;
    let permissions = r#"{"permissions":["propose_project","propose_task","read_project"]}"#;
    let agent_id = "characterization-project-agent".to_owned();
    let profile_id = insert_identity(&db, &agent_id, permissions).await;
    let project_id = "characterization-project".to_owned();
    let now = now_rfc3339();
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.clone(),
            name: "Characterization Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(USER_ID.to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("characterization project");

    let binding_id: String = sqlx::query_scalar(
        "SELECT id FROM project_agent_binding WHERE project_id = ? AND state = 'agent_setup_required'",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .expect("generated Project Agent binding");
    sqlx::query(
        "UPDATE project_agent_binding
         SET identity_id = ?, profile_id = ?, state = 'active',
             permission_ceiling_json = ?, wake_budget = 10, updated_at = ?
         WHERE id = ?",
    )
    .bind(&agent_id)
    .bind(&profile_id)
    .bind(permissions)
    .bind(&now)
    .bind(binding_id)
    .execute(db.pool())
    .await
    .expect("active Project Agent binding");

    ProjectFixture {
        db,
        project_id,
        agent_id,
    }
}

async fn main_fixture() -> (Arc<SqliteDb>, String, String) {
    let db = database().await;
    insert_user(&db).await;
    let permissions = r#"{"permissions":["propose_discovery","propose_project"]}"#;
    let main_agent_id = "characterization-main-agent".to_owned();
    let main_profile_id = insert_identity(&db, &main_agent_id, permissions).await;
    let now = now_rfc3339();
    let main_chat_id: String = sqlx::query_scalar(
        "SELECT id FROM agent_chat WHERE account_id = ? AND kind = 'account_main' LIMIT 1",
    )
    .bind(USER_ID)
    .fetch_one(db.pool())
    .await
    .expect("generated Main Chat");
    sqlx::query("UPDATE agent_chat SET status = 'ready' WHERE id = ?")
        .bind(&main_chat_id)
        .execute(db.pool())
        .await
        .expect("ready Main Chat");
    sqlx::query(
        "INSERT INTO account_main_agent_binding
         (id, account_id, identity_id, profile_id, state, autonomy_policy_json,
          tool_policy_revision, version, created_at, updated_at)
         VALUES ('characterization-main-binding', ?, ?, ?, 'active', '{}', 'default', 1, ?, ?)",
    )
    .bind(USER_ID)
    .bind(&main_agent_id)
    .bind(&main_profile_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("active Main binding");
    sqlx::query(
        "INSERT INTO product_genesis_session
         (id, account_id, main_chat_id, prompt_revision, prompt_body, maturity,
          lifecycle, source_message_ids_json, version, created_at, updated_at)
         VALUES ('characterization-genesis', ?, ?, 'prompt-r1', 'Characterization',
                 'mvp', 'discovering', '[]', 1, ?, ?)",
    )
    .bind(USER_ID)
    .bind(&main_chat_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("Genesis session");
    (db, main_chat_id, main_agent_id)
}

struct ActionInput<'a> {
    id: &'a str,
    actor_identity_id: &'a str,
    scope_type: &'a str,
    scope_id: &'a str,
    operation: &'a str,
    payload: &'a Value,
    target_type: &'a str,
    target_id: &'a str,
}

async fn create_action(db: &SqliteDb, input: ActionInput<'_>) -> AgentAction {
    let ActionInput {
        id,
        actor_identity_id,
        scope_type,
        scope_id,
        operation,
        payload,
        target_type,
        target_id,
    } = input;
    let now = now_rfc3339();
    let requested_permission = if operation == "task.propose" {
        "propose_task"
    } else {
        "propose_project"
    };
    AgentActionRepo::create_action(
        db,
        CreateAgentAction {
            id: id.to_owned(),
            actor_identity_id: actor_identity_id.to_owned(),
            scope_type: scope_type.to_owned(),
            scope_id: scope_id.to_owned(),
            operation: operation.to_owned(),
            payload_json: payload.to_string(),
            payload_hash: format!("hash-{id}"),
            dedupe_key: format!("dedupe-{id}"),
            correlation_id: format!("correlation-{id}"),
            causation_id: None,
            causation_depth: 0,
            requested_permission: requested_permission.to_owned(),
            policy_result: AgentActionPolicyResult::Allowed,
            policy_reason: None,
            status: AgentActionStatus::Proposed,
            target_type: Some(target_type.to_owned()),
            target_id: Some(target_id.to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("characterization action")
}

async fn count(db: &SqliteDb, sql: &str, bind: Option<&str>) -> i64 {
    let mut query = sqlx::query_scalar::<_, i64>(sql);
    if let Some(bind) = bind {
        query = query.bind(bind);
    }
    query
        .fetch_one(db.pool())
        .await
        .expect("characterization count")
}

fn research_content() -> Value {
    json!({
        "question": "What is the smallest replay-safe boundary?",
        "decision_informed": "Command receipt design",
        "scope": "Current action materializers",
        "stopping_condition": "The receipt seam is observable",
        "findings": ["Domain rows commit before action receipts"],
        "evidence": [],
        "inferences": [],
        "alternatives": [],
        "uncertainty": [],
        "unresolved_questions": [],
        "affected_artifact_ids": [],
        "affected_decision_ids": []
    })
}

fn charter_content() -> Value {
    json!({
        "identity": {
            "working_name": "Characterization Charter",
            "slug_proposal": "characterization-charter",
            "one_line_vision": "Make stranded action effects visible.",
            "maturity": "mvp"
        },
        "problem_and_people": {
            "problem_or_opportunity": "Action receipts are committed after domain rows.",
            "target_users": ["Forge maintainers"]
        },
        "core_experience": {
            "primary_outcome": "A retry cannot duplicate a committed effect."
        },
        "scope": {
            "must_have_outcomes": ["Capture the pre-fix seam."],
            "explicit_non_goals": ["Gate A implementation"]
        },
        "success": {
            "acceptance_statements": ["A receipt is required for successful replay."]
        },
        "constraints_and_risks": {},
        "knowledge_ledger": {"items": []}
    })
}

async fn attach_approved_charter_and_milestone(db: &SqliteDb, project_id: &str) {
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO project_charter
         (id, account_id, project_id, project_mode, maturity, lifecycle, version, created_at, updated_at)
         VALUES ('characterization-charter', ?, ?, 'compact', 'prototype', 'attached', 1, ?, ?)",
    )
    .bind(USER_ID)
    .bind(project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("baseline Charter");
    sqlx::query(
        "INSERT INTO project_charter_revision
         (id, charter_id, revision, base_revision, lifecycle, schema_version,
          render_version, content_json, rendered_view, change_summary, author_type,
          author_id, source_refs_json, content_digest, rendered_digest, created_at)
         VALUES ('characterization-charter-r1', 'characterization-charter', 1, 0,
                 'approved', 'charter-v1', 'charter-render-v1', '{}', '{}',
                 'characterization fixture', 'user', ?, '[]',
                 'charter-content-digest', 'charter-render-digest', ?)",
    )
    .bind(USER_ID)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("approved Charter revision");
    sqlx::query(
        "UPDATE project_charter
         SET current_draft_revision_id = 'characterization-charter-r1',
             current_approved_revision_id = 'characterization-charter-r1', version = 2
         WHERE id = 'characterization-charter'",
    )
    .execute(db.pool())
    .await
    .expect("Charter pointers");
    sqlx::query(
        "UPDATE project
         SET current_charter_id = 'characterization-charter',
             current_charter_revision_id = 'characterization-charter-r1',
             current_charter_version = 1, charter_status = 'charter_backed',
             charter_setup_required = 0, updated_at = ?
         WHERE id = ?",
    )
    .bind(&now)
    .bind(project_id)
    .execute(db.pool())
    .await
    .expect("Project Charter pointer");

    sqlx::query(
        "INSERT INTO project_milestone
         (id, project_id, milestone_sequence, milestone_key, display_label,
          lifecycle, blocker_reason_json, stale_reason_json, reconciliation_reason_json,
          version, created_at, updated_at)
         VALUES ('characterization-milestone', ?, 1, 'M001', 'Characterization milestone',
                 'planned', '[]', '[]', '[]', 1, ?, ?)",
    )
    .bind(project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("baseline milestone");
    sqlx::query(
        "INSERT INTO project_milestone_revision
         (id, milestone_id, revision, base_revision, lifecycle, display_label, outcome,
          included_scope_json, excluded_scope_json, charter_revision_id,
          document_revisions_json, task_selection_json, dependencies_json, risks_json,
          acceptance_checks_json, evidence_requirements_json, known_issues_json,
          change_summary, schema_version, render_version, rendered_view,
          content_digest, rendered_digest, author_type, author_id, source_refs_json, created_at)
         VALUES ('characterization-milestone-r1', 'characterization-milestone', 1, 0,
                 'proposed', 'Characterization milestone', 'Observe the seam',
                 '[]', '[]', 'characterization-charter-r1', '[]', '[]', '[]', '[]',
                 '[]', '[]', '[]', 'characterization fixture',
                 'forge.milestone-definition/v1', 'forge.milestone-definition-render/v1', '{}',
                 'milestone-content-digest', 'milestone-render-digest', 'user', ?, '[]', ?)",
    )
    .bind(USER_ID)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("milestone definition revision");
    sqlx::query(
        "UPDATE project_milestone
         SET current_definition_revision_id = 'characterization-milestone-r1'
         WHERE id = 'characterization-milestone'",
    )
    .execute(db.pool())
    .await
    .expect("milestone pointer");
}

fn baseline_content() -> ExecutionBaselineContent {
    let release_policy = ExecutionBaselineReleasePolicy {
        schema_version: crate::EXECUTION_BASELINE_RELEASE_POLICY_SCHEMA.to_owned(),
        revision: "policy-r1".to_owned(),
        required_check_definition_revisions: vec!["check-r1".to_owned()],
        reviewer_independence_rules: vec!["independent-reviewer".to_owned()],
        manual_attestation_rules: vec!["manual-attestation".to_owned()],
        waiver_rules: vec!["user-waiver".to_owned()],
        evidence_kinds: vec!["test-report".to_owned()],
        evidence_contexts: vec!["repository".to_owned()],
        evidence_freshness_rules: vec!["current-commit".to_owned()],
        dependency_rules: vec!["dependencies-green".to_owned()],
        stale_input_rules: vec!["stale-baseline-blocks".to_owned()],
        forbidden_side_effects: vec!["publish".to_owned()],
        known_issue_rules: vec!["record-known-issue".to_owned()],
        correction_rules: vec!["correct-before-release".to_owned()],
        purge_rules: vec!["purge-invalid-evidence".to_owned()],
    };
    let release_policy_digest = api_types::canonical_digest_with_schema(
        crate::EXECUTION_BASELINE_RELEASE_POLICY_SCHEMA,
        &release_policy,
    )
    .expect("release policy digest");
    ExecutionBaselineContent {
        charter_revision: ArtifactRef {
            artifact_id: "characterization-charter".to_owned(),
            revision_id: "characterization-charter-r1".to_owned(),
            content_digest: "charter-content-digest".to_owned(),
            render_version: Some("charter-render-v1".to_owned()),
            render_digest: Some("charter-render-digest".to_owned()),
        },
        document_revisions: Vec::new(),
        plan_item_ids: vec!["plan-1".to_owned()],
        milestone_ids: vec!["characterization-milestone".to_owned()],
        milestone_definition_revision_ids: vec!["characterization-milestone-r1".to_owned()],
        primary_milestone_id: Some("characterization-milestone".to_owned()),
        release_policy_revision: release_policy.revision.clone(),
        release_policy_digest,
        release_policy,
        acceptance_evidence_matrix: Vec::new(),
        capability_classes: vec!["repository_write".to_owned()],
        risk_classes: vec!["low".to_owned()],
        reviewer_independence_rules: Vec::new(),
        elevated_operations: Vec::new(),
        adaptive_envelope: AdaptiveEnvelope {
            allowed_task_operations: vec!["split".to_owned()],
            fixed_outcomes: Vec::new(),
            fixed_acceptance: Vec::new(),
            fixed_risk_classes: vec!["low".to_owned()],
            forbidden_side_effects: Vec::new(),
            elevated_operations: Vec::new(),
        },
        rollback_and_recovery: Vec::new(),
        exclusions: Vec::new(),
    }
}

#[tokio::test]
async fn document_shell_replays_frozen_response_before_membership_and_rejects_changed_input() {
    let fixture = project_fixture().await;
    let service = ProjectArtifactCommandService::new(Arc::clone(&fixture.db));
    let command = |title: &str, idempotency_key: &str| ProjectDocumentCreateCommand {
        project_id: fixture.project_id.clone(),
        kind: "research".to_owned(),
        title: title.to_owned(),
        approval_policy: "user_or_project_agent".to_owned(),
        expected_project_version: 1,
        idempotency_key: idempotency_key.to_owned(),
        authorization: document_create_authorization("document-shell-event"),
    };

    let first = service
        .create_document(command("Frozen shell", "document-shell-replay"), None)
        .await
        .expect("shell create");
    sqlx::query(
        "INSERT INTO project_document_revision (
            id, document_id, revision, base_revision, base_revision_id, lifecycle,
            schema_version, render_version, content_json, rendered_view,
            change_summary, author_type, author_id, source_refs_json,
            content_digest, rendered_digest, created_at
         ) VALUES ('later-revision', ?, 1, 0, NULL, 'approved',
                   'document@1', 'render@1', ?, '# Later', 'later',
                   'user', ?, '[]', 'later-content', 'later-render', ?)",
    )
    .bind(&first.id)
    .bind(research_content().to_string())
    .bind(USER_ID)
    .bind("2026-08-20T00:00:08.000Z")
    .execute(fixture.db.pool())
    .await
    .expect("later approved revision");
    sqlx::query("UPDATE project SET owner_id = NULL WHERE id = ?")
        .bind(&fixture.project_id)
        .execute(fixture.db.pool())
        .await
        .expect("remove owner after the receipt committed");
    sqlx::query(
        "UPDATE project_document
         SET lifecycle = 'approved', current_draft_revision_id = 'later-revision',
             current_approved_revision_id = 'later-revision', version = 9,
             updated_at = '2026-08-20T00:00:09.000Z'
         WHERE id = ?",
    )
    .bind(&first.id)
    .execute(fixture.db.pool())
    .await
    .expect("mutate the shell after its receipt committed");

    let replay = service
        .create_document(command("Frozen shell", "document-shell-replay"), None)
        .await
        .expect("exact replay before current membership");
    assert_eq!(replay, first);

    let changed = service
        .create_document(command("Changed shell", "document-shell-replay"), None)
        .await
        .expect_err("changed input must conflict before membership admission");
    assert!(matches!(
        changed,
        ServiceError::Db(db::DbError::IdempotencyConflict)
    ));

    let unauthorized = service
        .create_document(command("New shell", "document-shell-unauthorized"), None)
        .await
        .expect_err("ownerless project requires membership");
    assert!(matches!(
        unauthorized,
        ServiceError::AuthorizationDenied { .. }
    ));
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM project_document", None).await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM domain_event WHERE event_type = 'project.document.created'",
            None,
        )
        .await,
        1
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM command_receipt", None).await,
        1
    );
}

#[tokio::test]
async fn document_commit_before_receipt_replays_the_atomic_command_receipt() {
    let fixture = project_fixture().await;
    let payload = json!({
        "action": "draft_revision",
        "document_id": "document-placeholder",
        "kind": "research",
        "title": "Replay seam",
        "content": research_content()
    });
    let action = create_action(
        &fixture.db,
        ActionInput {
            id: "characterization-document-action",
            actor_identity_id: &fixture.agent_id,
            scope_type: "project",
            scope_id: &fixture.project_id,
            operation: PROJECT_DOCUMENT_OPERATION,
            payload: &payload,
            target_type: "project",
            target_id: &fixture.project_id,
        },
    )
    .await;
    let service = ProjectOrchestrationActionService::new(Arc::clone(&fixture.db));

    arm_after_domain_commit(&action.id);
    let stopped = service
        .execute(ExecuteProjectOrchestrationActionInput {
            action_id: action.id.clone(),
            expected_version: action.version,
            executed_by_type: "agent".to_owned(),
            executed_by_id: fixture.agent_id.clone(),
            idempotency_key: "characterization-document-replay".to_owned(),
        })
        .await
        .expect_err("failpoint stops before receipt");
    assert!(stopped.to_string().contains("characterization failpoint"));
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM project_document", None,).await,
        1,
        "the document shell is committed exactly once"
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM project_document_revision",
            None,
        )
        .await,
        1,
        "the first revision is committed with the shell"
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM domain_event WHERE event_type = 'project.document.revision_created'",
            None,
        )
        .await,
        1,
        "the domain event is committed with the command"
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM command_receipt", None,).await,
        1,
        "the command receipt is committed with the domain rows"
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM agent_action_execution",
            None,
        )
        .await,
        1,
        "the AgentAction execution is committed with the receipt"
    );

    let frozen_outcome: String =
        sqlx::query_scalar("SELECT outcome_json FROM command_receipt LIMIT 1")
            .fetch_one(fixture.db.pool())
            .await
            .expect("frozen command outcome");
    sqlx::query(
        "UPDATE project_agent_binding SET state = 'paused'
         WHERE project_id = ? AND identity_id = ?",
    )
    .bind(&fixture.project_id)
    .bind(&fixture.agent_id)
    .execute(fixture.db.pool())
    .await
    .expect("pause the current Project Agent binding after commit");
    let replay = service
        .execute(ExecuteProjectOrchestrationActionInput {
            action_id: action.id,
            expected_version: 1,
            executed_by_type: "agent".to_owned(),
            executed_by_id: fixture.agent_id,
            idempotency_key: "characterization-document-replay".to_owned(),
        })
        .await
        .expect("the response-loss retry replays the committed execution");
    assert_eq!(
        replay.result_json.as_deref(),
        Some(frozen_outcome.as_str()),
        "the replay returns the frozen original outcome"
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM project_document", None,).await,
        1,
        "the retry does not create a duplicate document"
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM project_document_revision",
            None,
        )
        .await,
        1
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM command_receipt", None,).await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM agent_action_execution",
            None,
        )
        .await,
        1
    );
}

#[tokio::test]
async fn charter_revision_response_loss_replays_the_atomic_command_receipt() {
    let (db, main_chat_id, main_agent_id) = main_fixture().await;
    let content: ProjectCharterContent =
        serde_json::from_value(charter_content()).expect("typed characterization Charter");
    let request = MainGenesisCharterDraftRequest {
        genesis_session_id: Some("characterization-genesis".to_owned()),
        charter_id: "characterization-charter".to_owned(),
        expected_charter_version: Some(1),
        base_revision_id: None,
        project_mode: ProjectMode::Compact,
        maturity: ProductMaturity::Mvp,
        content,
        change_summary: Some("Characterize the direct Charter command receipt seam".to_owned()),
        source_refs: vec![ProvenanceRef {
            source_kind: ProvenanceSourceKind::MainChat,
            source_id: main_chat_id.clone(),
            revision_id: None,
            digest: None,
            label: Some("characterization Main Chat".to_owned()),
            observed_at: None,
        }],
        provenance: RevisionProvenance {
            author: PrincipalRef {
                kind: PrincipalKind::Agent,
                id: main_agent_id.clone(),
                display_name: None,
            },
            profile_revision: None,
            operating_skill_revision: None,
            source_refs: vec![ProvenanceRef {
                source_kind: ProvenanceSourceKind::MainChat,
                source_id: main_chat_id.clone(),
                revision_id: None,
                digest: None,
                label: Some("characterization Main Chat".to_owned()),
                observed_at: None,
            }],
            change_summary: "Characterize the direct Charter command receipt seam".to_owned(),
            material_diff: None,
        },
        rendered_view: None,
        render_version: None,
        content_digest: None,
        render_digest: None,
    };
    let service = MainGenesisCommandService::new(Arc::clone(&db));
    let command = |idempotency_key: &str| MainGenesisDraftCommandInput {
        principal: MainGenesisDraftPrincipal::MainAgent {
            identity_id: main_agent_id.clone(),
            scope: CanonicalScope {
                scope_type: CanonicalScopeType::AgentChat,
                scope_id: main_chat_id.clone(),
                workspace_access: WorkspaceAccess::Deny,
            },
        },
        request: request.clone(),
        idempotency_key: idempotency_key.to_owned(),
        correlation_id: "characterization-charter-replay-correlation".to_owned(),
        causation_id: None,
        causation_depth: 0,
        policy_result: "allowed".to_owned(),
        requested_permission: "propose_discovery".to_owned(),
    };

    arm_after_domain_commit("characterization-charter-replay");
    let stopped = service
        .execute(command("characterization-charter-replay"))
        .await
        .expect_err("failpoint simulates response loss after the atomic commit");
    assert!(stopped.to_string().contains("characterization failpoint"));
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM project_charter_revision", None,).await,
        1
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM project_charter", None).await,
        1,
        "the direct command creates one Charter shell with its revision"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM domain_event", None).await,
        1,
        "the domain event commits with the Charter revision"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM agent_action", None).await,
        0,
        "a direct Charter command never enters the AgentAction bus"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM agent_action_execution", None).await,
        0,
        "a direct Charter command has no AgentAction execution"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM command_receipt", None).await,
        1,
        "the command receipt commits with the Charter revision"
    );
    let committed_receipt_id: String = sqlx::query_scalar(
        "SELECT id FROM command_receipt WHERE operation = ? AND idempotency_key = ?",
    )
    .bind(MAIN_CHARTER_DRAFT_OPERATION)
    .bind("characterization-charter-replay")
    .fetch_one(db.pool())
    .await
    .expect("committed direct Charter receipt");
    let committed_revision_id: String =
        sqlx::query_scalar("SELECT id FROM project_charter_revision WHERE charter_id = ?")
            .bind("characterization-charter")
            .fetch_one(db.pool())
            .await
            .expect("committed direct Charter revision");
    let committed_event_id: String = sqlx::query_scalar(
        "SELECT event_id FROM command_receipt WHERE operation = ? AND idempotency_key = ?",
    )
    .bind(MAIN_CHARTER_DRAFT_OPERATION)
    .bind("characterization-charter-replay")
    .fetch_one(db.pool())
    .await
    .expect("committed direct Charter event");
    let retry_service = MainGenesisCommandService::new(Arc::clone(&db));
    let retry = retry_service
        .execute(command("characterization-charter-replay"))
        .await
        .expect("the committed command replays despite mutable Charter state");
    assert_eq!(retry.receipt_id, committed_receipt_id);
    assert_eq!(retry.event_id, committed_event_id);
    assert_eq!(retry.revision.id, committed_revision_id);
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM project_charter_revision", None,).await,
        1,
        "response-loss replay does not duplicate the Charter revision"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM project_charter", None).await,
        1,
        "response-loss replay does not duplicate the Charter shell"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM domain_event", None).await,
        1,
        "response-loss replay does not duplicate the domain event"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM agent_action_execution", None).await,
        0,
        "response-loss replay does not create an AgentAction execution"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM command_receipt", None).await,
        1,
        "response-loss replay returns the frozen receipt"
    );
}

#[tokio::test]
async fn charter_draft_receipt_trigger_rolls_back_everything_and_retry_succeeds() {
    let (db, main_chat_id, main_agent_id) = main_fixture().await;
    let content: ProjectCharterContent =
        serde_json::from_value(charter_content()).expect("typed characterization Charter");
    let request = MainGenesisCharterDraftRequest {
        genesis_session_id: Some("characterization-genesis".to_owned()),
        charter_id: "characterization-charter".to_owned(),
        expected_charter_version: Some(1),
        base_revision_id: None,
        project_mode: ProjectMode::Compact,
        maturity: ProductMaturity::Mvp,
        content,
        change_summary: Some("Characterize the direct Charter trigger rollback".to_owned()),
        source_refs: vec![ProvenanceRef {
            source_kind: ProvenanceSourceKind::MainChat,
            source_id: main_chat_id.clone(),
            revision_id: None,
            digest: None,
            label: Some("characterization Main Chat".to_owned()),
            observed_at: None,
        }],
        provenance: RevisionProvenance {
            author: PrincipalRef {
                kind: PrincipalKind::Agent,
                id: main_agent_id.clone(),
                display_name: None,
            },
            profile_revision: None,
            operating_skill_revision: None,
            source_refs: vec![ProvenanceRef {
                source_kind: ProvenanceSourceKind::MainChat,
                source_id: main_chat_id.clone(),
                revision_id: None,
                digest: None,
                label: Some("characterization Main Chat".to_owned()),
                observed_at: None,
            }],
            change_summary: "Characterize the direct Charter trigger rollback".to_owned(),
            material_diff: None,
        },
        rendered_view: None,
        render_version: None,
        content_digest: None,
        render_digest: None,
    };
    let command = |idempotency_key: &str| MainGenesisDraftCommandInput {
        principal: MainGenesisDraftPrincipal::MainAgent {
            identity_id: main_agent_id.clone(),
            scope: CanonicalScope {
                scope_type: CanonicalScopeType::AgentChat,
                scope_id: main_chat_id.clone(),
                workspace_access: WorkspaceAccess::Deny,
            },
        },
        request: request.clone(),
        idempotency_key: idempotency_key.to_owned(),
        correlation_id: "characterization-charter-trigger-correlation".to_owned(),
        causation_id: None,
        causation_depth: 0,
        policy_result: "allowed".to_owned(),
        requested_permission: "propose_discovery".to_owned(),
    };
    let idempotency_key = "characterization-charter-trigger";
    let trigger = format!(
        "CREATE TEMP TRIGGER main_charter_receipt_failpoint
         BEFORE INSERT ON command_receipt
         WHEN NEW.operation = '{MAIN_CHARTER_DRAFT_OPERATION}'
         BEGIN SELECT RAISE(ABORT, 'main Charter receipt failpoint'); END;"
    );
    sqlx::query(&trigger)
        .execute(db.pool())
        .await
        .expect("Main Charter receipt failpoint");

    let service = MainGenesisCommandService::new(Arc::clone(&db));
    let stopped = service
        .execute(command(idempotency_key))
        .await
        .expect_err("receipt trigger stops the direct Charter command");
    assert!(
        stopped
            .to_string()
            .contains("main Charter receipt failpoint"),
        "unexpected Main Charter trigger error: {stopped}"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM project_charter", None).await,
        0,
        "Charter shell rolls back with the receipt"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM project_charter_revision", None).await,
        0,
        "Charter revision rolls back with the receipt"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM domain_event", None).await,
        0,
        "Main Charter domain event rolls back with the receipt"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM command_receipt", None).await,
        0,
        "failed Main Charter command leaves no receipt"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM agent_action", None).await,
        0,
        "direct Main Charter command has no AgentAction residue"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM agent_action_execution", None).await,
        0,
        "direct Main Charter command has no action execution residue"
    );

    sqlx::query("DROP TRIGGER main_charter_receipt_failpoint")
        .execute(db.pool())
        .await
        .expect("remove Main Charter receipt failpoint");
    let retry_service = MainGenesisCommandService::new(Arc::clone(&db));
    let committed = retry_service
        .execute(command(idempotency_key))
        .await
        .expect("retry succeeds after the receipt trigger is removed");
    assert!(!committed.receipt_id.is_empty());
    assert!(!committed.event_id.is_empty());
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM project_charter", None).await,
        1
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM project_charter_revision", None).await,
        1
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM domain_event", None).await,
        1
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM command_receipt", None).await,
        1
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM agent_action", None).await,
        0
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM agent_action_execution", None).await,
        0
    );
    let replay = retry_service
        .execute(command(idempotency_key))
        .await
        .expect("retry of the committed Main Charter command replays");
    assert_eq!(replay.receipt_id, committed.receipt_id);
    assert_eq!(replay.event_id, committed.event_id);
    assert_eq!(replay.revision.id, committed.revision.id);
}

#[tokio::test]
async fn baseline_revision_commit_before_receipt_is_duplicate_on_retry_pre_gate_a() {
    let fixture = project_fixture().await;
    attach_approved_charter_and_milestone(&fixture.db, &fixture.project_id).await;
    let content = baseline_content();
    let rendered = crate::render_execution_baseline(&content).expect("render baseline");
    let payload = json!({
        "action": "draft_revision",
        "content": content,
        "rendered_view": rendered.rendered_view,
        "render_version": crate::EXECUTION_BASELINE_RENDER_VERSION,
        "content_digest": rendered.content_digest,
        "render_digest": rendered.render_digest,
        "provenance": {
            "author": {
                "kind": "agent",
                "id": fixture.agent_id,
            },
            "source_refs": [],
            "change_summary": "characterize native baseline command",
        },
    });
    let action = create_action(
        &fixture.db,
        ActionInput {
            id: "characterization-baseline-action",
            actor_identity_id: &fixture.agent_id,
            scope_type: "project",
            scope_id: &fixture.project_id,
            operation: PROJECT_EXECUTION_BASELINE_OPERATION,
            payload: &payload,
            target_type: "project",
            target_id: &fixture.project_id,
        },
    )
    .await;
    let service = ProjectOrchestrationActionService::new(Arc::clone(&fixture.db));

    arm_after_domain_commit(&action.id);
    let stopped = service
        .execute(ExecuteProjectOrchestrationActionInput {
            action_id: action.id.clone(),
            expected_version: action.version,
            executed_by_type: "agent".to_owned(),
            executed_by_id: fixture.agent_id.clone(),
            idempotency_key: "characterization-baseline-replay".to_owned(),
        })
        .await
        .expect_err("failpoint stops before receipt");
    assert!(stopped.to_string().contains("characterization failpoint"));
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM project_execution_baseline_revision",
            None,
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM agent_action_execution",
            None
        )
        .await,
        1,
        "atomic baseline command commits its action receipt"
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt WHERE scope_type = 'project' AND scope_id = ?",
            Some(&fixture.project_id),
        )
        .await,
        1,
        "atomic baseline command commits its command receipt"
    );

    service
        .execute(ExecuteProjectOrchestrationActionInput {
            action_id: action.id,
            expected_version: 1,
            executed_by_type: "agent".to_owned(),
            executed_by_id: fixture.agent_id,
            idempotency_key: "characterization-baseline-replay".to_owned(),
        })
        .await
        .expect("response-loss retry returns the frozen action execution");
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM project_execution_baseline",
            None,
        )
        .await,
        1,
        "response-loss replay does not create another baseline shell"
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM project_execution_baseline_revision",
            None,
        )
        .await,
        1,
        "response-loss replay does not create another baseline revision"
    );
}

#[tokio::test]
async fn task_proposal_failure_rolls_back_and_retry_replays_exact_receipt() {
    let fixture = project_fixture().await;
    let payload = json!({
        "title": "Characterize task receipt seam",
        "description": "The Task commits before the action receipt.",
        "task_type": "planning_task"
    });
    let action = create_action(
        &fixture.db,
        ActionInput {
            id: "characterization-task-action",
            actor_identity_id: &fixture.agent_id,
            scope_type: "project",
            scope_id: &fixture.project_id,
            operation: "task.propose",
            payload: &payload,
            target_type: "project",
            target_id: &fixture.project_id,
        },
    )
    .await;
    let action_service = AgentActionService::new(Arc::clone(&fixture.db));
    let task_service = TaskService::new(Arc::clone(&fixture.db), Arc::new(EventBus::new(16)));

    // Fail at the old seam where the Task used to be committed before the
    // action/command receipt.  The atomic TaskService command must roll back
    // every authoritative row instead of stranding a Task that a retry could
    // duplicate.
    sqlx::query(
        "CREATE TEMP TRIGGER task_proposal_receipt_failpoint
         BEFORE INSERT ON command_receipt
         WHEN NEW.operation = 'task.propose'
         BEGIN SELECT RAISE(ABORT, 'task proposal failpoint'); END;",
    )
    .execute(fixture.db.pool())
    .await
    .expect("task proposal receipt failpoint");
    let stopped = action_service
        .execute_task_proposal(
            &task_service,
            ExecuteTaskProposalInput {
                action_id: action.id.clone(),
                expected_version: action.version,
                executed_by_type: "agent".to_owned(),
                executed_by_id: fixture.agent_id.clone(),
                idempotency_key: "characterization-task-replay".to_owned(),
            },
        )
        .await
        .expect_err("failpoint stops before receipt");
    assert!(
        stopped.to_string().contains("task proposal failpoint"),
        "unexpected task proposal failpoint error: {stopped}"
    );
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM task", None).await,
        0,
        "the Task rolls back with its receipt"
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM agent_action_execution",
            None
        )
        .await,
        0,
        "action execution rolls back with the Task"
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'task.propose'",
            None,
        )
        .await,
        0,
        "the failed command has no success receipt"
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM domain_event WHERE event_type = 'task.created'",
            None,
        )
        .await,
        0,
        "the durable event rolls back with the Task"
    );

    sqlx::query("DROP TRIGGER task_proposal_receipt_failpoint")
        .execute(fixture.db.pool())
        .await
        .expect("remove task proposal receipt failpoint");

    let first = action_service
        .execute_task_proposal(
            &task_service,
            ExecuteTaskProposalInput {
                action_id: action.id.clone(),
                expected_version: 1,
                executed_by_type: "agent".to_owned(),
                executed_by_id: fixture.agent_id.clone(),
                idempotency_key: "characterization-task-replay".to_owned(),
            },
        )
        .await
        .expect("retry is admitted after rollback");
    let replay = action_service
        .execute_task_proposal(
            &task_service,
            ExecuteTaskProposalInput {
                action_id: action.id,
                expected_version: 1,
                executed_by_type: "agent".to_owned(),
                executed_by_id: fixture.agent_id,
                idempotency_key: "characterization-task-replay".to_owned(),
            },
        )
        .await
        .expect("response-loss retry replays the frozen Task");
    assert_eq!(first.task.id, replay.task.id);
    assert_eq!(first.execution.id, replay.execution.id);
    assert_eq!(
        count(&fixture.db, "SELECT COUNT(*) FROM task", None).await,
        1,
        "the response-loss retry does not create a duplicate Task"
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM agent_action_execution",
            None
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fixture.db,
            "SELECT COUNT(*) FROM command_receipt WHERE operation = 'task.propose'",
            None,
        )
        .await,
        1
    );
}
