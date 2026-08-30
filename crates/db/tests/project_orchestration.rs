use db::{
    create_sqlite_pool, run_migrations, AgentActionPolicyResult, AgentActionRepo,
    AgentActionStatus, AgentRepo, AgentStatus, CreateAgentAction, CreateAgentActionExecution,
    CreateAgentIdentity, CreateAgentProfile, CreateCommandReceipt, CreateDomainEvent,
    CreateProject, CreateProjectCanonicalConflict, CreateProjectCharter,
    CreateProjectCharterRevision, CreateProjectFromCharterApproval, CreateProjectReconciliation,
    DbError, ProjectOrchestrationRepo, ResolveProjectReconciliation, SqliteDb, User, UserRepo,
};
use sha2::{Digest, Sha256};
use sqlx::Row;
use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

const ACCOUNT_ID: &str = "orchestration-account";
const MAIN_IDENTITY_ID: &str = "orchestration-main-identity";
const MAIN_PROFILE_ID: &str = "orchestration-main-profile";
const PROJECT_AGENT_IDENTITY_ID: &str = "orchestration-project-identity";
const PROJECT_AGENT_PROFILE_ID: &str = "orchestration-project-profile";
const PROJECT_SKILL_KEY: &str = "forge.project.orchestration/v1";
const PROJECT_POLICY_REVISION: &str = "policy@1";
const PROJECT_POLICY_DIGEST: &str =
    "289884035ab841815b521543c9b203dfb06e9a5c2bd787aeb0ce51936586d44e";

fn digest(value: &str) -> String {
    Sha256::digest(value.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Resolve the current active Project operating-skill revision the same way
/// the approval route does: the approval contract requires the selected
/// revision to be the skill's *current* revision, which migrations may
/// advance (e.g. V084 repointed `forge.project.orchestration/v1` to `@2`).
async fn project_skill_revision_id(db: &SqliteDb) -> String {
    sqlx::query_scalar(
        "SELECT revision.id
         FROM operating_skill AS skill
         JOIN operating_skill_revision AS revision
           ON revision.id = skill.current_revision_id
          AND revision.operating_skill_id = skill.id
          AND revision.skill_key = skill.skill_key
         WHERE skill.skill_key = ?
           AND skill.lifecycle = 'active'
           AND skill.current_revision_id IS NOT NULL
         LIMIT 1",
    )
    .bind(PROJECT_SKILL_KEY)
    .fetch_one(db.pool())
    .await
    .expect("current Project operating skill revision")
}

async fn fixture() -> (SqliteDb, String, String, String) {
    fixture_with_url("sqlite::memory:").await
}

async fn fixture_with_url(url: &str) -> (SqliteDb, String, String, String) {
    let pool = create_sqlite_pool(url).await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    let db = SqliteDb::new(pool);
    let now = "2026-08-13T00:00:00.000Z";
    UserRepo::create_user(
        &db,
        &User {
            id: ACCOUNT_ID.to_owned(),
            email: "orchestration@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Orchestration Test".to_owned()),
            is_admin: false,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("account");

    AgentRepo::create_identity_with_profile(
        &db,
        CreateAgentIdentity {
            id: MAIN_IDENTITY_ID.to_owned(),
            name: "Main Agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: true,
            paused: false,
            owner_id: Some(ACCOUNT_ID.to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
        CreateAgentProfile {
            id: MAIN_PROFILE_ID.to_owned(),
            identity_id: MAIN_IDENTITY_ID.to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test-model".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("Main identity");
    AgentRepo::create_identity_with_profile(
        &db,
        CreateAgentIdentity {
            id: PROJECT_AGENT_IDENTITY_ID.to_owned(),
            name: "Project Agent".to_owned(),
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
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
        CreateAgentProfile {
            id: PROJECT_AGENT_PROFILE_ID.to_owned(),
            identity_id: PROJECT_AGENT_IDENTITY_ID.to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test-model".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("Project identity");

    let main_chat_id: String = sqlx::query_scalar(
        "SELECT id FROM agent_chat WHERE kind = 'account_main' AND account_id = ?",
    )
    .bind(ACCOUNT_ID)
    .fetch_one(db.pool())
    .await
    .expect("Main Chat");
    sqlx::query("UPDATE agent_chat SET status = 'ready' WHERE id = ?")
        .bind(&main_chat_id)
        .execute(db.pool())
        .await
        .expect("Main Chat ready");

    let genesis_id = "orchestration-genesis";
    sqlx::query(
        "INSERT INTO product_genesis_session
            (id, account_id, main_chat_id, prompt_revision, prompt_body, maturity,
             initial_idea, lifecycle, source_message_ids_json, version, created_at, updated_at)
         VALUES (?, ?, ?, 'prompt@1', 'Build a compact Project', 'mvp',
                 'Build a compact Project', 'discovering', '[]', 1, ?, ?)",
    )
    .bind(genesis_id)
    .bind(ACCOUNT_ID)
    .bind(&main_chat_id)
    .bind(now)
    .bind(now)
    .execute(db.pool())
    .await
    .expect("Genesis");

    (db, genesis_id.to_owned(), main_chat_id, now.to_owned())
}

async fn file_fixture() -> (SqliteDb, String, String, String, PathBuf) {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "forge-project-create-race-{}-{nanos}.db",
        std::process::id()
    ));
    let (db, genesis_id, main_chat_id, now) =
        fixture_with_url(&format!("sqlite://{}", path.display())).await;
    (db, genesis_id, main_chat_id, now, path)
}

async fn approval_fixture(db: &SqliteDb, genesis_id: &str, now: &str) -> (String, String) {
    let charter_id = "orchestration-charter";
    ProjectOrchestrationRepo::create_project_charter(
        db,
        CreateProjectCharter {
            id: charter_id.to_owned(),
            account_id: ACCOUNT_ID.to_owned(),
            genesis_session_id: Some(genesis_id.to_owned()),
            project_mode: "compact".to_owned(),
            maturity: "mvp".to_owned(),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("Charter");
    let content_json =
        r#"{"success":{"acceptance_statements":["The delivered outcome is usable."]}}"#;
    let rendered_view = "# Compact Project\n\nThe delivered outcome is usable.";
    let revision_id = "orchestration-charter-revision-1";
    ProjectOrchestrationRepo::create_project_charter_revision(
        db,
        CreateProjectCharterRevision {
            id: revision_id.to_owned(),
            charter_id: charter_id.to_owned(),
            expected_charter_version: 1,
            project_mode: "compact".to_owned(),
            maturity: "mvp".to_owned(),
            base_revision: 0,
            base_revision_id: None,
            lifecycle: "proposed".to_owned(),
            schema_version: "forge.project-charter/v1".to_owned(),
            render_version: "1".to_owned(),
            content_json: content_json.to_owned(),
            rendered_view: rendered_view.to_owned(),
            change_summary: "Initial Charter".to_owned(),
            author_type: "user".to_owned(),
            author_id: Some(ACCOUNT_ID.to_owned()),
            source_message_id: None,
            source_turn_job_id: None,
            source_refs_json: "[]".to_owned(),
            content_digest: digest(content_json),
            rendered_digest: digest(rendered_view),
            created_at: now.to_owned(),
            command_receipt: None,
            action_execution: None,
        },
    )
    .await
    .expect("Charter revision");
    let approval_id = "orchestration-approval";
    let skill_revision_id = project_skill_revision_id(db).await;
    ProjectOrchestrationRepo::approve_project_charter(
        db,
        db::ApproveProjectCharter {
            id: approval_id.to_owned(),
            approval_type: "project_creation".to_owned(),
            charter_id: charter_id.to_owned(),
            revision_id: revision_id.to_owned(),
            content_digest: digest(content_json),
            rendered_digest: digest(rendered_view),
            expected_charter_version: 2,
            approved_name: Some("Compact Orchestration Project".to_owned()),
            approved_slug: Some("compact-orchestration-project".to_owned()),
            approved_project_mode: "compact".to_owned(),
            selected_identity_id: Some(PROJECT_AGENT_IDENTITY_ID.to_owned()),
            selected_profile_id: Some(PROJECT_AGENT_PROFILE_ID.to_owned()),
            selected_operating_skill_revision_id: Some(skill_revision_id),
            selected_policy_revision: Some(PROJECT_POLICY_REVISION.to_owned()),
            selected_policy_digest: Some(PROJECT_POLICY_DIGEST.to_owned()),
            approving_principal_type: "user".to_owned(),
            approving_principal_id: ACCOUNT_ID.to_owned(),
            authorization_basis: "explicit user approval".to_owned(),
            authorization_action: "project.charter.approve".to_owned(),
            explicit_event: "approve exact Charter".to_owned(),
            authorization_occurred_at: now.to_owned(),
            source_action: "product_genesis.approve_charter".to_owned(),
            idempotency_key: "charter-approval-key".to_owned(),
            event_id: "charter-approval-event".to_owned(),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("Charter approval");
    (charter_id.to_owned(), revision_id.to_owned())
}

fn create_input(
    approval_id: &str,
    project_id: &str,
    handoff_id: &str,
    target_message_id: &str,
    target_turn_id: &str,
    now: &str,
    source_revisions_json: &str,
) -> CreateProjectFromCharterApproval {
    CreateProjectFromCharterApproval {
        approval_id: approval_id.to_owned(),
        idempotency_key: "project-create-key".to_owned(),
        account_id: ACCOUNT_ID.to_owned(),
        project: CreateProject {
            id: project_id.to_owned(),
            name: "Compact Orchestration Project".to_owned(),
            settings:
                r#"{"project_mode":"compact","charter_schema_version":"forge.project-charter/v1"}"#
                    .to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(ACCOUNT_ID.to_owned()),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
        project_agent_binding_id: "orchestration-project-binding".to_owned(),
        handoff_id: handoff_id.to_owned(),
        target_message_id: target_message_id.to_owned(),
        target_turn_id: target_turn_id.to_owned(),
        source_identity_id: Some(MAIN_IDENTITY_ID.to_owned()),
        source_profile_id: Some(MAIN_PROFILE_ID.to_owned()),
        source_instruction_revision_id: None,
        source_message_id: None,
        source_turn_id: None,
        handoff_content: "Approved handoff".to_owned(),
        content_guard_json: "{}".to_owned(),
        source_revisions_json: source_revisions_json.to_owned(),
        create_principal_type: "user".to_owned(),
        create_principal_id: ACCOUNT_ID.to_owned(),
        create_authorization_basis: "explicit user executed Project creation".to_owned(),
        create_action: "product_genesis.create_project_from_approval".to_owned(),
        create_event_id: "project-create-event".to_owned(),
        create_occurred_at: now.to_owned(),
        correlation_id: "orchestration-correlation".to_owned(),
        causation_id: Some("orchestration-cause".to_owned()),
        causation_depth: 0,
        max_attempts: 3,
        provisioning_operation_id: db::new_uuid_v4(),
        policy_revision: PROJECT_POLICY_REVISION.to_owned(),
        policy_digest: PROJECT_POLICY_DIGEST.to_owned(),
        member_id: "orchestration-member".to_owned(),
        command_receipt: None,
        action_execution: None,
    }
}

fn project_create_receipt(
    id: &str,
    main_chat_id: &str,
    input_digest: &str,
    outcome: serde_json::Value,
) -> CreateCommandReceipt {
    CreateCommandReceipt {
        id: id.to_owned(),
        principal_type: "user".to_owned(),
        principal_id: ACCOUNT_ID.to_owned(),
        scope_type: "agent_chat".to_owned(),
        scope_id: main_chat_id.to_owned(),
        operation: "product_genesis.create_project_from_approval".to_owned(),
        idempotency_key: "project-create-key".to_owned(),
        input_digest: input_digest.to_owned(),
        policy_result: "allowed".to_owned(),
        correlation_id: "orchestration-race-correlation".to_owned(),
        causation_id: Some("orchestration-race-causation".to_owned()),
        causation_depth: 0,
        event_id: String::new(),
        agent_action_execution_id: None,
        outcome_json: outcome.to_string(),
        committed_at: "2026-08-13T00:00:00.000Z".to_owned(),
    }
}

#[tokio::test]
async fn charter_approval_create_is_atomic_and_replay_safe() {
    let (db, genesis_id, _main_chat_id, now) = fixture().await;
    let (charter_id, revision_id) = approval_fixture(&db, &genesis_id, &now).await;
    let source = r#"{"schema_version":"forge.project-charter-handoff/v1","project":{"id":"project-1","name":"Compact Orchestration Project","mode":"compact"},"target":{},"source":{"identity_id":"orchestration-main-identity","profile_revision_id":"orchestration-main-profile"}}"#;
    let input = create_input(
        "orchestration-approval",
        "project-1",
        "orchestration-handoff",
        "orchestration-target-message",
        "orchestration-target-turn",
        &now,
        source,
    );
    let created =
        ProjectOrchestrationRepo::create_project_from_charter_approval(&db, input.clone())
            .await
            .expect("atomic Project creation");
    assert_eq!(created.charter_id, charter_id);
    assert_eq!(created.charter_revision_id, revision_id);
    assert_eq!(created.project.id, "project-1");
    assert!(created.project.primary_milestone_id.is_some());
    let frozen_turn = sqlx::query(
        "SELECT responder_binding_id, responder_binding_version,
                responder_identity_version, profile_version,
                operating_skill_revision_id, policy_revision, policy_digest,
                permission_policy_digest, tool_policy_digest, admission_digest,
                canonical_scope_provenance_json, canonical_scope_id
         FROM agent_chat_turn_job WHERE id = ?",
    )
    .bind(&created.target_turn_id)
    .fetch_one(db.pool())
    .await
    .expect("Genesis target turn provenance");
    assert_eq!(
        frozen_turn
            .try_get::<String, _>("responder_binding_id")
            .expect("binding id"),
        created.project_agent_binding_id
    );
    for column in [
        "operating_skill_revision_id",
        "policy_revision",
        "policy_digest",
        "permission_policy_digest",
        "tool_policy_digest",
        "admission_digest",
        "canonical_scope_provenance_json",
    ] {
        assert!(
            !frozen_turn
                .try_get::<Option<String>, _>(column)
                .expect("frozen provenance column")
                .is_none(),
            "Genesis target turn must freeze {column}"
        );
    }
    for column in [
        "responder_binding_version",
        "responder_identity_version",
        "profile_version",
    ] {
        assert!(
            !frozen_turn
                .try_get::<Option<i64>, _>(column)
                .expect("frozen provenance version")
                .is_none(),
            "Genesis target turn must freeze {column}"
        );
    }
    assert_eq!(
        frozen_turn
            .try_get::<String, _>("canonical_scope_id")
            .expect("canonical scope id"),
        created.project_chat_id
    );
    let milestone_lifecycle: String = sqlx::query_scalar(
        "SELECT lifecycle FROM project_milestone WHERE project_id = ? AND milestone_key = 'M001'",
    )
    .bind(&created.project.id)
    .fetch_one(db.pool())
    .await
    .expect("compact bootstrap milestone");
    // Approving the Charter approves the work: M001 lands with its approved
    // definition and is active, with no second artifact to wait on.
    assert_eq!(milestone_lifecycle, "active");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_milestone
             WHERE project_id = ? AND lifecycle = 'active'",
        )
        .bind(&created.project.id)
        .fetch_one(db.pool())
        .await
        .expect("active milestone count"),
        1
    );

    let replay = ProjectOrchestrationRepo::create_project_from_charter_approval(&db, input)
        .await
        .expect("exact replay");
    assert_eq!(replay.project.id, created.project.id);
    assert_eq!(replay.project_chat_id, created.project_chat_id);
    assert_eq!(
        replay.project_agent_binding_id,
        created.project_agent_binding_id
    );
    assert_eq!(replay.handoff_id, created.handoff_id);
    assert_eq!(replay.target_message_id, created.target_message_id);
    assert_eq!(replay.target_turn_id, created.target_turn_id);
    assert!(sqlx::query(
        "UPDATE project_charter_approval
         SET consumed_project_id = 'tampered-project'
         WHERE id = 'orchestration-approval'",
    )
    .execute(db.pool())
    .await
    .is_err());
    assert!(sqlx::query(
        "UPDATE project_charter_approval
         SET consumed_at = 'tampered-time'
         WHERE id = 'orchestration-approval'",
    )
    .execute(db.pool())
    .await
    .is_err());
    let stored_packet: String = sqlx::query_scalar(
        "SELECT source_revisions_json FROM agent_handoff WHERE id = 'orchestration-handoff'",
    )
    .fetch_one(db.pool())
    .await
    .expect("stored handoff packet");
    let stored_packet: serde_json::Value =
        serde_json::from_str(&stored_packet).expect("stored packet JSON");
    assert_eq!(
        stored_packet["source"]["profile_revision_id"],
        "orchestration-main-profile"
    );
    assert!(stored_packet["source"].get("profile_id").is_none());
    assert_eq!(
        stored_packet["request"]["source_revisions_digest"]
            .as_str()
            .map(str::len),
        Some(64)
    );

    let counts: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM project WHERE id = 'project-1'),
            (SELECT COUNT(*) FROM agent_handoff WHERE id = 'orchestration-handoff'),
            (SELECT COUNT(*) FROM agent_chat_message WHERE id = 'orchestration-target-message'),
            (SELECT COUNT(*) FROM agent_chat_turn_job WHERE id = 'orchestration-target-turn')",
    )
    .fetch_one(db.pool())
    .await
    .expect("composite counts");
    assert_eq!(counts, (1, 1, 1, 1));

    let mut altered = create_input(
        "orchestration-approval",
        "project-1",
        "orchestration-handoff",
        "orchestration-target-message",
        "orchestration-target-turn",
        &now,
        source,
    );
    altered.idempotency_key = "different-create-key".to_owned();
    assert!(matches!(
        ProjectOrchestrationRepo::create_project_from_charter_approval(&db, altered).await,
        Err(DbError::VersionConflict)
    ));

    let conflict = ProjectOrchestrationRepo::create_project_canonical_conflict(
        &db,
        CreateProjectCanonicalConflict {
            id: "project-conflict".to_owned(),
            project_id: created.project.id.clone(),
            domain: "charter".to_owned(),
            governing_record_type: "project_charter_revision".to_owned(),
            governing_record_id: revision_id.clone(),
            governing_record_revision: "1".to_owned(),
            governing_record_digest: "digest-governing".to_owned(),
            conflicting_record_type: "project_document_revision".to_owned(),
            conflicting_record_id: "document-revision".to_owned(),
            conflicting_record_revision: "2".to_owned(),
            conflicting_record_digest: "digest-conflicting".to_owned(),
            affected_paths_json: r#"["/scope/outcome"]"#.to_owned(),
            conflict_code: "outcome_mismatch".to_owned(),
            description: "The approved outcome claims disagree.".to_owned(),
            detected_by_type: "system".to_owned(),
            detected_by_id: None,
            authorization_basis: "canonical state evaluator".to_owned(),
            authorization_action: "project.canonical_conflict.detect".to_owned(),
            explicit_event: "evaluate canonical state".to_owned(),
            authorization_occurred_at: now.clone(),
            idempotency_key: "project-conflict-key".to_owned(),
            created_at: now.clone(),
        },
    )
    .await
    .expect("canonical conflict");
    assert_eq!(conflict.project_id, created.project.id);
    let reconciliation = ProjectOrchestrationRepo::create_project_reconciliation(
        &db,
        CreateProjectReconciliation {
            id: "project-reconciliation".to_owned(),
            project_id: created.project.id.clone(),
            conflict_id: conflict.id.clone(),
            record_type: "project_document_revision".to_owned(),
            record_id: "document-revision".to_owned(),
            record_revision: "2".to_owned(),
            record_digest: "digest-conflicting".to_owned(),
            governing_record_type: "project_charter_revision".to_owned(),
            governing_record_id: revision_id,
            governing_record_revision: "1".to_owned(),
            governing_record_digest: "digest-governing".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("reconciliation projection");
    assert_eq!(reconciliation.state, "required");
    let resolved = ProjectOrchestrationRepo::resolve_project_reconciliation(
        &db,
        ResolveProjectReconciliation {
            id: reconciliation.id.clone(),
            expected_version: reconciliation.version,
            resolution_id: "project-resolution".to_owned(),
            action: "retained".to_owned(),
            principal_type: "user".to_owned(),
            principal_id: ACCOUNT_ID.to_owned(),
            authorization_basis: "explicit reconciliation decision".to_owned(),
            authorization_action: "project.reconciliation.resolve".to_owned(),
            explicit_event: "retain governing Charter".to_owned(),
            authorization_occurred_at: now.clone(),
            reason: "The Charter remains authoritative after review.".to_owned(),
            replacement_ref_type: None,
            replacement_ref_id: None,
            replacement_ref_revision: None,
            occurred_at: now.clone(),
            idempotency_key: "project-resolution-key".to_owned(),
            updated_at: now.clone(),
            domain_event: CreateDomainEvent {
                id: "project-reconciliation-resolved-event".to_owned(),
                event_type: "project.reconciliation.resolved".to_owned(),
                entity_type: "project_reconciliation".to_owned(),
                entity_id: "project-reconciliation".to_owned(),
                actor_type: "user".to_owned(),
                actor_id: Some(ACCOUNT_ID.to_owned()),
                scope_type: "project".to_owned(),
                scope_id: created.project.id.clone(),
                correlation_id: "project-resolution-correlation".to_owned(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: Some(
                    "project.reconciliation.resolved:project-reconciliation-key".to_owned(),
                ),
                payload_json: r#"{"action":"retained"}"#.to_owned(),
                created_at: now.clone(),
            },
            command_receipt: CreateCommandReceipt {
                id: "project-resolution-receipt".to_owned(),
                principal_type: "user".to_owned(),
                principal_id: ACCOUNT_ID.to_owned(),
                scope_type: "project".to_owned(),
                scope_id: created.project.id.clone(),
                operation: "project.reconciliation.resolve".to_owned(),
                idempotency_key: "project-resolution-key".to_owned(),
                input_digest: "project-resolution-input-digest".to_owned(),
                policy_result: "allowed".to_owned(),
                correlation_id: "project-resolution-correlation".to_owned(),
                causation_id: None,
                causation_depth: 0,
                event_id: "project-reconciliation-resolved-event".to_owned(),
                agent_action_execution_id: None,
                outcome_json: r#"{"state":"retained"}"#.to_owned(),
                committed_at: now.clone(),
            },
        },
    )
    .await
    .expect("explicit reconciliation");
    assert_eq!(resolved.state, "retained");
    assert!(resolved.current_resolution_id.is_some());
}

#[tokio::test]
async fn main_project_create_finalizes_action_and_receipt_in_the_composite_transaction() {
    let (db, genesis_id, main_chat_id, now) = fixture().await;
    let (charter_id, revision_id) = approval_fixture(&db, &genesis_id, &now).await;
    let source = r#"{"schema_version":"forge.project-charter-handoff/v1","project":{"id":"project-command","name":"Compact Orchestration Project","mode":"compact"},"target":{},"source":{"identity_id":"orchestration-main-identity","profile_revision_id":"orchestration-main-profile"}}"#;
    let mut input = create_input(
        "orchestration-approval",
        "project-command",
        "orchestration-command-handoff",
        "orchestration-command-target-message",
        "orchestration-command-target-turn",
        &now,
        source,
    );
    let action_id = "orchestration-main-project-action";
    let action = AgentActionRepo::create_action(
        &db,
        CreateAgentAction {
            id: action_id.to_owned(),
            actor_identity_id: MAIN_IDENTITY_ID.to_owned(),
            scope_type: "agent_chat".to_owned(),
            scope_id: main_chat_id.clone(),
            operation: "product_genesis.create_project_from_approval".to_owned(),
            payload_json: r#"{"approval_id":"orchestration-approval"}"#.to_owned(),
            payload_hash: "command-payload-digest".to_owned(),
            dedupe_key: "orchestration-main-project-action-dedupe".to_owned(),
            correlation_id: "orchestration-command-correlation".to_owned(),
            causation_id: None,
            causation_depth: 0,
            requested_permission: "propose_project".to_owned(),
            policy_result: AgentActionPolicyResult::Allowed,
            policy_reason: None,
            status: AgentActionStatus::Proposed,
            target_type: Some("project".to_owned()),
            target_id: Some("project-command".to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("Main project action");
    let placeholder =
        r#"{"operation":"product_genesis.create_project_from_approval","pending":true}"#;
    let execution_id = "orchestration-main-project-execution";
    input.action_execution = Some(CreateAgentActionExecution {
        id: execution_id.to_owned(),
        action_id: action.id.clone(),
        expected_action_version: action.version,
        attempt: 1,
        status: db::AgentActionExecutionStatus::Succeeded,
        result_json: Some(placeholder.to_owned()),
        error: None,
        executed_by_type: "user".to_owned(),
        executed_by_id: ACCOUNT_ID.to_owned(),
        idempotency_key: input.idempotency_key.clone(),
        action_status: AgentActionStatus::Executed,
        action_outcome_json: Some(placeholder.to_owned()),
        created_at: now.clone(),
        completed_at: Some(now.clone()),
        updated_at: now.clone(),
    });
    input.command_receipt = Some(CreateCommandReceipt {
        id: "orchestration-main-project-receipt".to_owned(),
        principal_type: "user".to_owned(),
        principal_id: ACCOUNT_ID.to_owned(),
        scope_type: "agent_chat".to_owned(),
        scope_id: main_chat_id,
        operation: "product_genesis.create_project_from_approval".to_owned(),
        idempotency_key: input.idempotency_key.clone(),
        input_digest: "canonical-command-digest".to_owned(),
        policy_result: "allowed".to_owned(),
        correlation_id: "orchestration-command-correlation".to_owned(),
        causation_id: None,
        causation_depth: 0,
        event_id: String::new(),
        agent_action_execution_id: Some(execution_id.to_owned()),
        outcome_json: placeholder.to_owned(),
        committed_at: now.clone(),
    });

    let created =
        ProjectOrchestrationRepo::create_project_from_charter_approval(&db, input.clone())
            .await
            .expect("atomic Main project command");
    let expected = serde_json::json!({
        "operation": "product_genesis.create_project_from_approval",
        "project_id": created.project.id,
        "project_agent_binding_id": created.project_agent_binding_id,
        "project_chat_id": created.project_chat_id,
        "charter_id": charter_id,
        "charter_revision_id": revision_id,
        "handoff_id": created.handoff_id,
        "target_message_id": created.target_message_id,
        "target_turn_id": created.target_turn_id,
    });
    let (stored_outcome, stored_event_id, stored_execution_id): (String, String, String) =
        sqlx::query_as(
            "SELECT outcome_json, event_id, agent_action_execution_id
             FROM command_receipt WHERE id = 'orchestration-main-project-receipt'",
        )
        .fetch_one(db.pool())
        .await
        .expect("Main project receipt");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&stored_outcome).expect("outcome JSON"),
        expected
    );
    assert_eq!(stored_execution_id, execution_id);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM domain_event WHERE id = ?")
            .bind(stored_event_id)
            .fetch_one(db.pool())
            .await
            .expect("project event"),
        1
    );
    let (execution_result, execution_status): (String, String) =
        sqlx::query_as("SELECT result_json, status FROM agent_action_execution WHERE id = ?")
            .bind(execution_id)
            .fetch_one(db.pool())
            .await
            .expect("project execution");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&execution_result).unwrap(),
        expected
    );
    assert_eq!(execution_status, "succeeded");

    let replay = ProjectOrchestrationRepo::create_project_from_charter_approval(&db, input)
        .await
        .expect("response-loss replay");
    assert_eq!(replay.project.id, created.project.id);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project WHERE id = 'project-command'",)
            .fetch_one(db.pool())
            .await
            .expect("one project"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_action_execution WHERE action_id = ?",
        )
        .bind(action_id)
        .fetch_one(db.pool())
        .await
        .expect("one execution"),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_main_project_create_replay_uses_the_first_frozen_ids() {
    let (db, genesis_id, main_chat_id, now, path) = file_fixture().await;
    let (charter_id, revision_id) = approval_fixture(&db, &genesis_id, &now).await;
    let source = r#"{"schema_version":"forge.project-charter-handoff/v1","project":{"name":"Compact Orchestration Project","mode":"compact"},"target":{},"source":{"identity_id":"orchestration-main-identity","profile_revision_id":"orchestration-main-profile"}}"#;
    let outcome = |project_id: &str,
                   binding_id: &str,
                   chat_id: &str,
                   handoff_id: &str,
                   message_id: &str,
                   turn_id: &str| {
        serde_json::json!({
            "operation": "product_genesis.create_project_from_approval",
            "project_id": project_id,
            "project_agent_binding_id": binding_id,
            "project_chat_id": chat_id,
            "charter_id": charter_id,
            "charter_revision_id": revision_id,
            "handoff_id": handoff_id,
            "target_message_id": message_id,
            "target_turn_id": turn_id,
        })
    };
    let mut first = create_input(
        "orchestration-approval",
        "race-project-first",
        "race-handoff-first",
        "race-message-first",
        "race-turn-first",
        &now,
        source,
    );
    first.command_receipt = Some(project_create_receipt(
        "race-receipt-first",
        &main_chat_id,
        "project-create-race-digest",
        outcome(
            "race-project-first",
            "race-binding-first",
            "race-chat-first",
            "race-handoff-first",
            "race-message-first",
            "race-turn-first",
        ),
    ));
    let mut second = create_input(
        "orchestration-approval",
        "race-project-second",
        "race-handoff-second",
        "race-message-second",
        "race-turn-second",
        &now,
        source,
    );
    second.project_agent_binding_id = "race-binding-second".to_owned();
    second.command_receipt = Some(project_create_receipt(
        "race-receipt-second",
        &main_chat_id,
        "project-create-race-digest",
        outcome(
            "race-project-second",
            "race-binding-second",
            "race-chat-second",
            "race-handoff-second",
            "race-message-second",
            "race-turn-second",
        ),
    ));

    let first_retry = first.clone();
    let second_retry = second.clone();
    let (first_result, second_result) = tokio::join!(
        ProjectOrchestrationRepo::create_project_from_charter_approval(&db, first),
        ProjectOrchestrationRepo::create_project_from_charter_approval(&db, second),
    );
    let first_result = first_result.expect("first concurrent Main Project-create");
    let second_result = second_result.expect("second concurrent Main Project-create replay");
    assert_eq!(first_result.project, second_result.project);
    assert_eq!(
        first_result.project_agent_binding_id,
        second_result.project_agent_binding_id
    );
    assert_eq!(first_result.project_chat_id, second_result.project_chat_id);
    assert_eq!(first_result.handoff_id, second_result.handoff_id);
    assert_eq!(
        first_result.target_message_id,
        second_result.target_message_id
    );
    assert_eq!(first_result.target_turn_id, second_result.target_turn_id);
    assert_eq!(first_result.charter_id, charter_id);
    assert_eq!(first_result.charter_revision_id, revision_id);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project")
            .fetch_one(db.pool())
            .await
            .expect("one Project"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_handoff")
            .fetch_one(db.pool())
            .await
            .expect("one handoff"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_chat_message")
            .fetch_one(db.pool())
            .await
            .expect("one target message"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_chat_turn_job")
            .fetch_one(db.pool())
            .await
            .expect("one target turn"),
        1
    );

    let mut changed = first_retry;
    changed
        .command_receipt
        .as_mut()
        .expect("Main receipt")
        .input_digest = "project-create-changed-digest".to_owned();
    assert!(matches!(
        ProjectOrchestrationRepo::create_project_from_charter_approval(&db, changed).await,
        Err(DbError::IdempotencyConflict)
    ));
    let _ = second_retry;
    db.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn charter_approval_create_rolls_back_on_invalid_handoff_packet() {
    let (db, genesis_id, _main_chat_id, now) = fixture().await;
    approval_fixture(&db, &genesis_id, &now).await;
    let input = create_input(
        "orchestration-approval",
        "rolled-back-project",
        "rolled-back-handoff",
        "rolled-back-message",
        "rolled-back-turn",
        &now,
        "not-json",
    );
    assert!(matches!(
        ProjectOrchestrationRepo::create_project_from_charter_approval(&db, input).await,
        Err(DbError::Check(_))
    ));
    let project_count: i64 =
        sqlx::query("SELECT COUNT(*) AS count FROM project WHERE id = 'rolled-back-project'")
            .fetch_one(db.pool())
            .await
            .expect("project count")
            .get("count");
    assert_eq!(project_count, 0);
    let approval_state: String = sqlx::query_scalar(
        "SELECT lifecycle FROM project_charter_approval WHERE id = 'orchestration-approval'",
    )
    .fetch_one(db.pool())
    .await
    .expect("approval state");
    assert_eq!(approval_state, "active");
    let genesis_lifecycle: String = sqlx::query_scalar(
        "SELECT lifecycle FROM product_genesis_session WHERE id = 'orchestration-genesis'",
    )
    .fetch_one(db.pool())
    .await
    .expect("Genesis state");
    assert_eq!(genesis_lifecycle, "ready_for_project");
}

#[tokio::test]
async fn charter_approval_replay_ignores_transport_row_ids_but_rejects_changed_target_or_authorization(
) {
    let (db, genesis_id, _main_chat_id, now) = fixture().await;
    let (charter_id, revision_id) = approval_fixture(&db, &genesis_id, &now).await;
    let skill_revision_id = project_skill_revision_id(&db).await;
    let replay = ProjectOrchestrationRepo::approve_project_charter(
        &db,
        db::ApproveProjectCharter {
            id: "fresh-transport-approval-id".to_owned(),
            approval_type: "project_creation".to_owned(),
            charter_id,
            revision_id,
            content_digest: digest(
                r#"{"success":{"acceptance_statements":["The delivered outcome is usable."]}}"#,
            ),
            rendered_digest: digest("# Compact Project\n\nThe delivered outcome is usable."),
            expected_charter_version: 2,
            approved_name: Some("Compact Orchestration Project".to_owned()),
            approved_slug: Some("compact-orchestration-project".to_owned()),
            approved_project_mode: "compact".to_owned(),
            selected_identity_id: Some(PROJECT_AGENT_IDENTITY_ID.to_owned()),
            selected_profile_id: Some(PROJECT_AGENT_PROFILE_ID.to_owned()),
            selected_operating_skill_revision_id: Some(skill_revision_id.clone()),
            selected_policy_revision: Some(PROJECT_POLICY_REVISION.to_owned()),
            selected_policy_digest: Some(PROJECT_POLICY_DIGEST.to_owned()),
            approving_principal_type: "user".to_owned(),
            approving_principal_id: ACCOUNT_ID.to_owned(),
            authorization_basis: "explicit user approval".to_owned(),
            authorization_action: "project.charter.approve".to_owned(),
            explicit_event: "approve exact Charter".to_owned(),
            authorization_occurred_at: now.clone(),
            source_action: "product_genesis.approve_charter".to_owned(),
            idempotency_key: "charter-approval-key".to_owned(),
            // The approval/event row ids are transport-generated, but the
            // authorization event id is part of the exact replay envelope.
            event_id: "charter-approval-event".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("approval replay");
    assert_eq!(replay.id, "orchestration-approval");
    assert!(sqlx::query(
        "UPDATE project_charter_approval_event
         SET explicit_event = 'tampered' WHERE id = 'charter-approval-event'",
    )
    .execute(db.pool())
    .await
    .is_err());
    assert!(sqlx::query(
        "DELETE FROM project_charter_approval_event WHERE id = 'charter-approval-event'",
    )
    .execute(db.pool())
    .await
    .is_err());

    let changed = db::ApproveProjectCharter {
        id: "another-transport-approval-id".to_owned(),
        approval_type: "project_creation".to_owned(),
        charter_id: "orchestration-charter".to_owned(),
        revision_id: "orchestration-charter-revision-1".to_owned(),
        content_digest: digest(
            r#"{"success":{"acceptance_statements":["The delivered outcome is usable."]}}"#,
        ),
        rendered_digest: digest("# Compact Project\n\nThe delivered outcome is usable."),
        expected_charter_version: 2,
        approved_name: Some("A different approved name".to_owned()),
        approved_slug: Some("compact-orchestration-project".to_owned()),
        approved_project_mode: "compact".to_owned(),
        selected_identity_id: Some(PROJECT_AGENT_IDENTITY_ID.to_owned()),
        selected_profile_id: Some(PROJECT_AGENT_PROFILE_ID.to_owned()),
        selected_operating_skill_revision_id: Some(skill_revision_id),
        selected_policy_revision: Some(PROJECT_POLICY_REVISION.to_owned()),
        selected_policy_digest: Some(PROJECT_POLICY_DIGEST.to_owned()),
        approving_principal_type: "user".to_owned(),
        approving_principal_id: ACCOUNT_ID.to_owned(),
        authorization_basis: "explicit user approval".to_owned(),
        authorization_action: "project.charter.approve".to_owned(),
        explicit_event: "approve exact Charter".to_owned(),
        authorization_occurred_at: now.clone(),
        source_action: "product_genesis.approve_charter".to_owned(),
        idempotency_key: "charter-approval-key".to_owned(),
        event_id: "different-event".to_owned(),
        created_at: now.clone(),
        updated_at: now,
    };
    assert!(matches!(
        ProjectOrchestrationRepo::approve_project_charter(&db, changed).await,
        Err(DbError::VersionConflict)
    ));
}

#[tokio::test]
async fn charter_create_rechecks_selected_agent_availability_inside_transaction() {
    let (db, genesis_id, _main_chat_id, now) = fixture().await;
    approval_fixture(&db, &genesis_id, &now).await;
    sqlx::query("UPDATE agent_identity SET paused = 1 WHERE id = ?")
        .bind(PROJECT_AGENT_IDENTITY_ID)
        .execute(db.pool())
        .await
        .expect("pause selected Project Agent");
    let input = create_input(
        "orchestration-approval",
        "paused-project",
        "paused-handoff",
        "paused-message",
        "paused-turn",
        &now,
        r#"{"schema_version":"forge.project-charter-handoff/v1","project":{"id":"paused-project","name":"Compact Orchestration Project","mode":"compact"},"target":{},"source":{"identity_id":"orchestration-main-identity","profile_revision_id":"orchestration-main-profile"}}"#,
    );
    assert!(matches!(
        ProjectOrchestrationRepo::create_project_from_charter_approval(&db, input).await,
        Err(DbError::VersionConflict)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project WHERE id = 'paused-project'",)
            .fetch_one(db.pool())
            .await
            .expect("rolled back paused Project"),
        0
    );

    sqlx::query("UPDATE agent_identity SET paused = 0 WHERE id = ?")
        .bind(PROJECT_AGENT_IDENTITY_ID)
        .execute(db.pool())
        .await
        .expect("resume selected Project Agent");
    sqlx::query(
        "UPDATE operating_skill
         SET lifecycle = 'retired', current_revision_id = NULL
         WHERE skill_key = 'forge.project.orchestration/v1'",
    )
    .execute(db.pool())
    .await
    .expect("retire selected operating skill");
    let input = create_input(
        "orchestration-approval",
        "retired-skill-project",
        "retired-skill-handoff",
        "retired-skill-message",
        "retired-skill-turn",
        &now,
        r#"{"schema_version":"forge.project-charter-handoff/v1","project":{"id":"retired-skill-project","name":"Compact Orchestration Project","mode":"compact"},"target":{},"source":{"identity_id":"orchestration-main-identity","profile_revision_id":"orchestration-main-profile"}}"#,
    );
    assert!(matches!(
        ProjectOrchestrationRepo::create_project_from_charter_approval(&db, input).await,
        Err(DbError::VersionConflict)
    ));
}
