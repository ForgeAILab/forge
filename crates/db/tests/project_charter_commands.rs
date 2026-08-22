use db::{
    create_sqlite_pool, run_migrations, AgentRepo, AgentStatus, ApplyProjectCharterApprovalCommand,
    ApproveProjectCharter, CreateAgentIdentity, CreateAgentProfile, CreateCommandReceipt,
    CreateProject, CreateProjectCharter, CreateProjectCharterRevision,
    CreateProjectCharterRevisionAtomically, DbError, FinalizeProjectCharterRevisionNoop,
    ProjectOrchestrationRepo, ProjectRepo, SqliteDb, User, UserRepo,
};
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

const ACCOUNT_ID: &str = "charter-command-user";
const PROJECT_ID: &str = "charter-command-project";
const IDENTITY_ID: &str = "charter-command-agent";
const PROFILE_ID: &str = "charter-command-profile";
const CHARTER_ID: &str = "charter-command-charter";
const REVISION_ONE_ID: &str = "charter-command-revision-1";
const NOW: &str = "2026-08-20T00:00:00.000Z";
const PROJECT_SKILL_KEY: &str = "forge.project.orchestration/v1";
static FILE_FIXTURE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn policy_digest(policy: &str) -> String {
    let mut bytes = b"forge.project-agent-policy/v1\0".to_vec();
    bytes.extend_from_slice(policy.as_bytes());
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

async fn fixture() -> (SqliteDb, String) {
    fixture_with_url("sqlite::memory:").await
}

async fn fixture_with_url(url: &str) -> (SqliteDb, String) {
    let pool = create_sqlite_pool(url).await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    let db = SqliteDb::new(pool);
    UserRepo::create_user(
        &db,
        &User {
            id: ACCOUNT_ID.to_owned(),
            email: "charter-command@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Charter command".to_owned()),
            is_admin: false,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("user");
    AgentRepo::create_identity_with_profile(
        &db,
        CreateAgentIdentity {
            id: IDENTITY_ID.to_owned(),
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
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        CreateAgentProfile {
            id: PROFILE_ID.to_owned(),
            identity_id: IDENTITY_ID.to_owned(),
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
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("Project Agent");
    ProjectRepo::create(
        &db,
        CreateProject {
            id: PROJECT_ID.to_owned(),
            name: "Setup Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(ACCOUNT_ID.to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("Project");
    let skill_revision_id: String = sqlx::query_scalar(
        "SELECT revision.id
         FROM operating_skill skill
         JOIN operating_skill_revision revision
           ON revision.id = skill.current_revision_id
          AND revision.operating_skill_id = skill.id
         WHERE skill.skill_key = ? AND skill.lifecycle = 'active'
         LIMIT 1",
    )
    .bind(PROJECT_SKILL_KEY)
    .fetch_one(db.pool())
    .await
    .expect("current Project operating skill");
    ProjectOrchestrationRepo::create_project_charter_revision_atomically(
        &db,
        CreateProjectCharterRevisionAtomically {
            project_id: Some(PROJECT_ID.to_owned()),
            genesis_session_id: None,
            account_id: ACCOUNT_ID.to_owned(),
            charter: CreateProjectCharter {
                id: CHARTER_ID.to_owned(),
                account_id: ACCOUNT_ID.to_owned(),
                genesis_session_id: None,
                project_mode: "compact".to_owned(),
                maturity: "mvp".to_owned(),
                created_at: NOW.to_owned(),
                updated_at: NOW.to_owned(),
            },
            revision: CreateProjectCharterRevision {
                id: REVISION_ONE_ID.to_owned(),
                charter_id: CHARTER_ID.to_owned(),
                expected_charter_version: 1,
                project_mode: "compact".to_owned(),
                maturity: "mvp".to_owned(),
                base_revision: 0,
                base_revision_id: None,
                lifecycle: "draft".to_owned(),
                schema_version: "forge.project-charter/v1".to_owned(),
                render_version: "forge.project-charter-render/v1".to_owned(),
                content_json: r#"{"success":{"acceptance_statements":["Usable"]}}"#.to_owned(),
                rendered_view: "# Setup Project".to_owned(),
                change_summary: "initial Charter".to_owned(),
                author_type: "agent".to_owned(),
                author_id: Some(IDENTITY_ID.to_owned()),
                source_message_id: None,
                source_turn_job_id: None,
                source_refs_json: "[]".to_owned(),
                content_digest: "charter-content-1".to_owned(),
                rendered_digest: "charter-render-1".to_owned(),
                created_at: NOW.to_owned(),
                command_receipt: None,
                action_execution: None,
            },
            command_receipt: None,
            action_execution: None,
        },
    )
    .await
    .expect("first Charter revision");
    (db, skill_revision_id)
}

async fn file_fixture() -> (SqliteDb, String, PathBuf) {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after epoch")
        .as_nanos();
    let counter = FILE_FIXTURE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "forge-charter-race-{}-{nanos}-{counter}.db",
        std::process::id()
    ));
    let url = format!("sqlite://{}", path.display());
    let (db, skill_revision_id) = fixture_with_url(&url).await;
    (db, skill_revision_id, path)
}

fn approval(
    id: &str,
    approval_type: &str,
    revision_id: &str,
    expected_charter_version: i64,
    skill_revision_id: &str,
    event_id: &str,
) -> db::ApproveProjectCharter {
    ApproveProjectCharter {
        id: id.to_owned(),
        approval_type: approval_type.to_owned(),
        charter_id: CHARTER_ID.to_owned(),
        revision_id: revision_id.to_owned(),
        content_digest: if revision_id == REVISION_ONE_ID {
            "charter-content-1".to_owned()
        } else {
            "charter-content-2".to_owned()
        },
        rendered_digest: if revision_id == REVISION_ONE_ID {
            "charter-render-1".to_owned()
        } else {
            "charter-render-2".to_owned()
        },
        expected_charter_version,
        approved_name: Some("Adopted Project".to_owned()),
        approved_slug: Some("adopted-project".to_owned()),
        approved_project_mode: "compact".to_owned(),
        selected_identity_id: Some(IDENTITY_ID.to_owned()),
        selected_profile_id: Some(PROFILE_ID.to_owned()),
        selected_operating_skill_revision_id: Some(skill_revision_id.to_owned()),
        selected_policy_revision: Some("policy@1".to_owned()),
        selected_policy_digest: Some(policy_digest("{}")),
        approving_principal_type: "user".to_owned(),
        approving_principal_id: ACCOUNT_ID.to_owned(),
        authorization_basis: "explicit user approval".to_owned(),
        authorization_action: "project.charter.approve".to_owned(),
        explicit_event: format!("approve-{id}"),
        authorization_occurred_at: NOW.to_owned(),
        source_action: "project.charter.approve".to_owned(),
        idempotency_key: format!("approval-key-{id}"),
        event_id: event_id.to_owned(),
        created_at: NOW.to_owned(),
        updated_at: NOW.to_owned(),
    }
}

fn receipt(key: &str, outcome: serde_json::Value) -> CreateCommandReceipt {
    CreateCommandReceipt {
        id: format!("receipt-{key}"),
        principal_type: "user".to_owned(),
        principal_id: ACCOUNT_ID.to_owned(),
        scope_type: "project".to_owned(),
        scope_id: PROJECT_ID.to_owned(),
        operation: "project.charter.approval".to_owned(),
        idempotency_key: key.to_owned(),
        input_digest: format!("digest-{key}"),
        policy_result: "allowed".to_owned(),
        correlation_id: format!("correlation-{key}"),
        causation_id: Some(format!("cause-{key}")),
        causation_depth: 1,
        event_id: "pending-event".to_owned(),
        agent_action_execution_id: None,
        outcome_json: outcome.to_string(),
        committed_at: NOW.to_owned(),
    }
}

fn charter_revision_receipt(
    key: &str,
    scope_type: &str,
    scope_id: &str,
    outcome: serde_json::Value,
) -> CreateCommandReceipt {
    CreateCommandReceipt {
        id: format!("revision-receipt-{key}"),
        principal_type: "user".to_owned(),
        principal_id: ACCOUNT_ID.to_owned(),
        scope_type: scope_type.to_owned(),
        scope_id: scope_id.to_owned(),
        operation: "project.charter.adoption".to_owned(),
        idempotency_key: key.to_owned(),
        input_digest: format!("revision-digest-{key}"),
        policy_result: "allowed".to_owned(),
        correlation_id: format!("revision-correlation-{key}"),
        causation_id: Some(format!("revision-cause-{key}")),
        causation_depth: 0,
        event_id: String::new(),
        agent_action_execution_id: None,
        outcome_json: outcome.to_string(),
        committed_at: NOW.to_owned(),
    }
}

#[tokio::test]
async fn charter_adoption_is_atomic_replay_exact_and_scope_bound() {
    let (db, skill_revision_id) = fixture().await;
    let binding_id: String = sqlx::query_scalar(
        "SELECT id FROM project_agent_binding
         WHERE project_id = ? AND state = 'agent_setup_required'",
    )
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("setup binding");
    let bootstrap_message_id = "charter-adoption-bootstrap";
    let outcome = serde_json::json!({
        "project_id": PROJECT_ID,
        "charter_id": CHARTER_ID,
        "revision_id": REVISION_ONE_ID,
        "approval_id": "adoption-approval",
        "binding_id": binding_id,
        "bootstrap_message_id": bootstrap_message_id,
    });
    let input = ApplyProjectCharterApprovalCommand {
        approval: approval(
            "adoption-approval",
            "adoption",
            REVISION_ONE_ID,
            2,
            &skill_revision_id,
            "adoption-active-event",
        ),
        project_id: PROJECT_ID.to_owned(),
        expected_project_version: 1,
        expected_current_charter_revision_id: None,
        existing_binding_id: binding_id,
        replacement_binding_id: None,
        bootstrap_message_id: Some(bootstrap_message_id.to_owned()),
        bootstrap_content: Some("Charter adoption committed".to_owned()),
        bootstrap_content_guard_json: Some("{}".to_owned()),
        bootstrap_author_id: Some(ACCOUNT_ID.to_owned()),
        bootstrap_correlation_id: Some("adoption-correlation".to_owned()),
        bootstrap_source_metadata_json: Some("{}".to_owned()),
        amendment_id: None,
        amendment_rationale: None,
        amendment_material_diff_json: None,
        amendment_affected_records_json: None,
        command_receipt: Some(receipt("adoption-command", outcome)),
        action_execution: None,
    };
    let created =
        ProjectOrchestrationRepo::apply_project_charter_approval_command(&db, input.clone())
            .await
            .expect("adoption command");
    assert_eq!(created.project_id, PROJECT_ID);
    assert_eq!(created.project_charter_revision_id, REVISION_ONE_ID);
    assert_eq!(created.project_charter_status, "charter_backed");
    assert_eq!(created.project_version, 2);
    assert_eq!(
        created.bootstrap_message_id.as_deref(),
        Some(bootstrap_message_id)
    );
    assert_eq!(created.approval.lifecycle, "consumed");

    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event
         WHERE event_type = 'project.charter.approved' AND entity_id = ?",
    )
    .bind(CHARTER_ID)
    .fetch_one(db.pool())
    .await
    .expect("domain event count");
    let receipt_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM command_receipt
         WHERE scope_type = 'project' AND scope_id = ? AND operation = ?",
    )
    .bind(PROJECT_ID)
    .bind("project.charter.approval")
    .fetch_one(db.pool())
    .await
    .expect("receipt count");
    let approval_event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_charter_approval_event WHERE approval_id = ?",
    )
    .bind("adoption-approval")
    .fetch_one(db.pool())
    .await
    .expect("approval event count");
    assert_eq!(event_count, 1);
    assert_eq!(receipt_count, 1);
    assert_eq!(approval_event_count, 2);

    let replay = ProjectOrchestrationRepo::apply_project_charter_approval_command(&db, input)
        .await
        .expect("exact adoption replay");
    assert_eq!(replay, created);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_charter_adoption_replay_returns_the_first_approval() {
    let (db, skill_revision_id, path) = file_fixture().await;
    let binding_id: String = sqlx::query_scalar(
        "SELECT id FROM project_agent_binding
         WHERE project_id = ? AND state = 'agent_setup_required'",
    )
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("setup binding");
    let project_chat_id: String =
        sqlx::query_scalar("SELECT id FROM agent_chat WHERE kind = 'project' AND project_id = ?")
            .bind(PROJECT_ID)
            .fetch_one(db.pool())
            .await
            .expect("project chat");
    let first_outcome = serde_json::json!({
        "operation": "project.charter.approval",
        "project_id": PROJECT_ID,
        "charter_id": CHARTER_ID,
        "revision_id": REVISION_ONE_ID,
        "approval_id": "adoption-race-first",
        "project_agent_binding_id": binding_id,
        "project_chat_id": project_chat_id,
        "bootstrap_message_id": "adoption-race-message-first",
        "amendment_id": null,
    });
    let mut first_receipt = receipt("adoption-race", first_outcome);
    first_receipt.id = "receipt-adoption-race-first".to_owned();
    let second_outcome = serde_json::json!({
        "operation": "project.charter.approval",
        "project_id": PROJECT_ID,
        "charter_id": CHARTER_ID,
        "revision_id": REVISION_ONE_ID,
        "approval_id": "adoption-race-second",
        "project_agent_binding_id": binding_id,
        "project_chat_id": project_chat_id,
        "bootstrap_message_id": "adoption-race-message-second",
        "amendment_id": null,
    });
    let mut second_receipt = receipt("adoption-race", second_outcome);
    second_receipt.id = "receipt-adoption-race-second".to_owned();
    let first = ApplyProjectCharterApprovalCommand {
        approval: approval(
            "adoption-race-first",
            "adoption",
            REVISION_ONE_ID,
            2,
            &skill_revision_id,
            "adoption-race-event-first",
        ),
        project_id: PROJECT_ID.to_owned(),
        expected_project_version: 1,
        expected_current_charter_revision_id: None,
        existing_binding_id: binding_id.clone(),
        replacement_binding_id: None,
        bootstrap_message_id: Some("adoption-race-message-first".to_owned()),
        bootstrap_content: Some("First adoption".to_owned()),
        bootstrap_content_guard_json: Some("{}".to_owned()),
        bootstrap_author_id: Some(ACCOUNT_ID.to_owned()),
        bootstrap_correlation_id: Some("adoption-race-correlation-first".to_owned()),
        bootstrap_source_metadata_json: Some("{}".to_owned()),
        amendment_id: None,
        amendment_rationale: None,
        amendment_material_diff_json: None,
        amendment_affected_records_json: None,
        command_receipt: Some(first_receipt),
        action_execution: None,
    };
    let second = ApplyProjectCharterApprovalCommand {
        approval: approval(
            "adoption-race-second",
            "adoption",
            REVISION_ONE_ID,
            2,
            &skill_revision_id,
            "adoption-race-event-second",
        ),
        project_id: PROJECT_ID.to_owned(),
        expected_project_version: 1,
        expected_current_charter_revision_id: None,
        existing_binding_id: binding_id,
        replacement_binding_id: None,
        bootstrap_message_id: Some("adoption-race-message-second".to_owned()),
        bootstrap_content: Some("Second adoption".to_owned()),
        bootstrap_content_guard_json: Some("{}".to_owned()),
        bootstrap_author_id: Some(ACCOUNT_ID.to_owned()),
        bootstrap_correlation_id: Some("adoption-race-correlation-second".to_owned()),
        bootstrap_source_metadata_json: Some("{}".to_owned()),
        amendment_id: None,
        amendment_rationale: None,
        amendment_material_diff_json: None,
        amendment_affected_records_json: None,
        command_receipt: Some(second_receipt),
        action_execution: None,
    };
    let (first, second) = tokio::join!(
        ProjectOrchestrationRepo::apply_project_charter_approval_command(&db, first),
        ProjectOrchestrationRepo::apply_project_charter_approval_command(&db, second),
    );
    let first = first.expect("first adoption");
    let second = second.expect("concurrent adoption replay");
    assert_eq!(second, first);
    assert!(matches!(
        first.approval.id.as_str(),
        "adoption-race-first" | "adoption-race-second"
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_charter_approval
             WHERE id LIKE 'adoption-race-%'",
        )
        .fetch_one(db.pool())
        .await
        .expect("approval count"),
        1
    );
    db.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_existing_charter_revision_replay_returns_the_first_revision() {
    let (db, _skill_revision_id, path) = file_fixture().await;
    let revision_input =
        |revision_id: &str, receipt: CreateCommandReceipt| CreateProjectCharterRevision {
            id: revision_id.to_owned(),
            charter_id: CHARTER_ID.to_owned(),
            expected_charter_version: 2,
            project_mode: "compact".to_owned(),
            maturity: "mvp".to_owned(),
            base_revision: 1,
            base_revision_id: Some(REVISION_ONE_ID.to_owned()),
            lifecycle: "proposed".to_owned(),
            schema_version: "forge.project-charter/v1".to_owned(),
            render_version: "forge.project-charter-render/v1".to_owned(),
            content_json: r#"{"success":{"acceptance_statements":["Replayed"]}}"#.to_owned(),
            rendered_view: "# Replayed Project".to_owned(),
            change_summary: "concurrent revision replay".to_owned(),
            author_type: "user".to_owned(),
            author_id: Some(ACCOUNT_ID.to_owned()),
            source_message_id: None,
            source_turn_job_id: None,
            source_refs_json: "[]".to_owned(),
            content_digest: "charter-content-race".to_owned(),
            rendered_digest: "charter-render-race".to_owned(),
            created_at: NOW.to_owned(),
            command_receipt: Some(receipt),
            action_execution: None,
        };
    let outcome = |revision_id: &str| {
        serde_json::json!({
            "operation": "project.charter.adoption",
            "charter_id": CHARTER_ID,
            "revision_id": revision_id,
            "revision": 2,
        })
    };
    let first = revision_input(
        "charter-race-revision-first",
        charter_revision_receipt(
            "charter-race-revision",
            "project",
            PROJECT_ID,
            outcome("charter-race-revision-first"),
        ),
    );
    let second = revision_input(
        "charter-race-revision-second",
        charter_revision_receipt(
            "charter-race-revision",
            "project",
            PROJECT_ID,
            outcome("charter-race-revision-second"),
        ),
    );
    let (first, second) = tokio::join!(
        ProjectOrchestrationRepo::create_project_charter_revision(&db, first),
        ProjectOrchestrationRepo::create_project_charter_revision(&db, second),
    );
    let first = first.expect("first concurrent Charter revision");
    let second = second.expect("second concurrent Charter revision replay");
    assert_eq!(first, second);
    assert!(matches!(
        first.id.as_str(),
        "charter-race-revision-first" | "charter-race-revision-second"
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_charter_revision WHERE charter_id = ?",
        )
        .bind(CHARTER_ID)
        .fetch_one(db.pool())
        .await
        .expect("revision count"),
        2
    );
    db.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn semantic_charter_revision_noop_finalizes_one_receipt_and_event() {
    let (db, _skill_revision_id) = fixture().await;
    let outcome = serde_json::json!({
        "operation": "project.charter.adoption",
        "project_id": PROJECT_ID,
        "charter_id": CHARTER_ID,
        "revision_id": REVISION_ONE_ID,
        "revision": 1,
        "charter_version": 2,
        "content_digest": "charter-content-1",
        "render_digest": "charter-render-1",
        "semantic_noop": true,
    });
    let receipt = charter_revision_receipt("semantic-charter-noop", "project", PROJECT_ID, outcome);
    let input = FinalizeProjectCharterRevisionNoop {
        account_id: ACCOUNT_ID.to_owned(),
        project_id: PROJECT_ID.to_owned(),
        charter_id: CHARTER_ID.to_owned(),
        revision_id: REVISION_ONE_ID.to_owned(),
        content_digest: "charter-content-1".to_owned(),
        rendered_digest: "charter-render-1".to_owned(),
        command_receipt: receipt,
        action_execution: None,
    };
    let created =
        ProjectOrchestrationRepo::finalize_project_charter_revision_noop(&db, input.clone())
            .await
            .expect("semantic no-op finalizes");
    let replay = ProjectOrchestrationRepo::finalize_project_charter_revision_noop(&db, input)
        .await
        .expect("same-key no-op replays");
    assert_eq!(replay, created);

    let revision_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM project_charter_revision WHERE charter_id = ?")
            .bind(CHARTER_ID)
            .fetch_one(db.pool())
            .await
            .expect("revision count");
    assert_eq!(revision_count, 1);
    let receipt_event_id: String = sqlx::query_scalar(
        "SELECT event_id FROM command_receipt
         WHERE operation = 'project.charter.adoption' AND idempotency_key = ?",
    )
    .bind("semantic-charter-noop")
    .fetch_one(db.pool())
    .await
    .expect("no-op receipt");
    let event_type: String = sqlx::query_scalar("SELECT event_type FROM domain_event WHERE id = ?")
        .bind(receipt_event_id)
        .fetch_one(db.pool())
        .await
        .expect("no-op event");
    assert_eq!(event_type, "project_charter.revision_noop");
    let receipt_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM command_receipt WHERE idempotency_key = ?")
            .bind("semantic-charter-noop")
            .fetch_one(db.pool())
            .await
            .expect("receipt count");
    assert_eq!(receipt_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_first_project_charter_shell_revision_replay_returns_frozen_ids() {
    let (db, _skill_revision_id, path) = file_fixture().await;
    let genesis_id = "charter-race-genesis";
    let main_chat_id: String = sqlx::query_scalar(
        "SELECT id FROM agent_chat WHERE kind = 'account_main' AND account_id = ?",
    )
    .bind(ACCOUNT_ID)
    .fetch_one(db.pool())
    .await
    .expect("Main Chat");
    sqlx::query(
        "INSERT INTO product_genesis_session
            (id, account_id, main_chat_id, prompt_revision, prompt_body, maturity,
             initial_idea, lifecycle, source_message_ids_json, version, created_at, updated_at)
         VALUES (?, ?, ?, 'prompt@1', 'Draft a Charter', 'mvp',
                 'Draft a Charter', 'discovering', '[]', 1, ?, ?)",
    )
    .bind(genesis_id)
    .bind(ACCOUNT_ID)
    .bind(&main_chat_id)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("Genesis");

    let input = |charter_id: &str, revision_id: &str, receipt: CreateCommandReceipt| {
        CreateProjectCharterRevisionAtomically {
            project_id: None,
            genesis_session_id: Some(genesis_id.to_owned()),
            account_id: ACCOUNT_ID.to_owned(),
            charter: CreateProjectCharter {
                id: charter_id.to_owned(),
                account_id: ACCOUNT_ID.to_owned(),
                genesis_session_id: Some(genesis_id.to_owned()),
                project_mode: "compact".to_owned(),
                maturity: "mvp".to_owned(),
                created_at: NOW.to_owned(),
                updated_at: NOW.to_owned(),
            },
            revision: CreateProjectCharterRevision {
                id: revision_id.to_owned(),
                charter_id: charter_id.to_owned(),
                expected_charter_version: 1,
                project_mode: "compact".to_owned(),
                maturity: "mvp".to_owned(),
                base_revision: 0,
                base_revision_id: None,
                lifecycle: "draft".to_owned(),
                schema_version: "forge.project-charter/v1".to_owned(),
                render_version: "forge.project-charter-render/v1".to_owned(),
                content_json: r#"{"success":{"acceptance_statements":["Genesis output"]}}"#
                    .to_owned(),
                rendered_view: "# Genesis Charter".to_owned(),
                change_summary: "concurrent Genesis Charter draft".to_owned(),
                author_type: "user".to_owned(),
                author_id: Some(ACCOUNT_ID.to_owned()),
                source_message_id: None,
                source_turn_job_id: None,
                source_refs_json: "[]".to_owned(),
                content_digest: "genesis-charter-content-race".to_owned(),
                rendered_digest: "genesis-charter-render-race".to_owned(),
                created_at: NOW.to_owned(),
                command_receipt: None,
                action_execution: None,
            },
            command_receipt: Some(receipt),
            action_execution: None,
        }
    };
    let outcome = |charter_id: &str, revision_id: &str| {
        serde_json::json!({
            "operation": "product_genesis.charter.draft",
            "charter_id": charter_id,
            "revision_id": revision_id,
            "revision": 1,
        })
    };
    let first = input(
        "charter-race-shell-first",
        "charter-race-shell-revision-first",
        charter_revision_receipt(
            "charter-race-shell",
            "account",
            ACCOUNT_ID,
            outcome(
                "charter-race-shell-first",
                "charter-race-shell-revision-first",
            ),
        ),
    );
    let second = input(
        "charter-race-shell-second",
        "charter-race-shell-revision-second",
        charter_revision_receipt(
            "charter-race-shell",
            "account",
            ACCOUNT_ID,
            outcome(
                "charter-race-shell-second",
                "charter-race-shell-revision-second",
            ),
        ),
    );
    let (first, second) = tokio::join!(
        ProjectOrchestrationRepo::create_project_charter_revision_atomically(&db, first),
        ProjectOrchestrationRepo::create_project_charter_revision_atomically(&db, second),
    );
    let first = first.expect("first concurrent Charter shell");
    let second = second.expect("second concurrent Charter shell replay");
    assert_eq!(first, second);
    assert!(matches!(
        first.charter_id.as_str(),
        "charter-race-shell-first" | "charter-race-shell-second"
    ));
    assert!(matches!(
        first.id.as_str(),
        "charter-race-shell-revision-first" | "charter-race-shell-revision-second"
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_charter WHERE genesis_session_id = ?",
        )
        .bind(genesis_id)
        .fetch_one(db.pool())
        .await
        .expect("one Genesis Charter"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_charter_revision WHERE charter_id = ?",
        )
        .bind(first.charter_id.clone())
        .fetch_one(db.pool())
        .await
        .expect("one Genesis revision"),
        1
    );
    db.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn charter_amendment_supersedes_execution_and_changed_replay_rolls_back() {
    let (db, skill_revision_id) = fixture().await;
    let setup_binding_id: String = sqlx::query_scalar(
        "SELECT id FROM project_agent_binding
         WHERE project_id = ? AND state = 'agent_setup_required'",
    )
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("setup binding");
    let adoption_outcome = serde_json::json!({
        "project_id": PROJECT_ID,
        "charter_id": CHARTER_ID,
        "revision_id": REVISION_ONE_ID,
        "approval_id": "adoption-approval-2",
        "binding_id": "adoption-binding-2",
        "bootstrap_message_id": "adoption-bootstrap-2",
    });
    let adoption = ApplyProjectCharterApprovalCommand {
        approval: approval(
            "adoption-approval-2",
            "adoption",
            REVISION_ONE_ID,
            2,
            &skill_revision_id,
            "adoption-event-2",
        ),
        project_id: PROJECT_ID.to_owned(),
        expected_project_version: 1,
        expected_current_charter_revision_id: None,
        existing_binding_id: setup_binding_id,
        replacement_binding_id: None,
        bootstrap_message_id: Some("adoption-bootstrap-2".to_owned()),
        bootstrap_content: Some("Adopted".to_owned()),
        bootstrap_content_guard_json: Some("{}".to_owned()),
        bootstrap_author_id: Some(ACCOUNT_ID.to_owned()),
        bootstrap_correlation_id: Some("adoption-2".to_owned()),
        bootstrap_source_metadata_json: Some("{}".to_owned()),
        amendment_id: None,
        amendment_rationale: None,
        amendment_material_diff_json: None,
        amendment_affected_records_json: None,
        command_receipt: Some(receipt("adoption-command-2", adoption_outcome)),
        action_execution: None,
    };
    let adopted = ProjectOrchestrationRepo::apply_project_charter_approval_command(&db, adoption)
        .await
        .expect("adoption");

    ProjectOrchestrationRepo::create_project_charter_revision(
        &db,
        CreateProjectCharterRevision {
            id: "charter-command-revision-2".to_owned(),
            charter_id: CHARTER_ID.to_owned(),
            expected_charter_version: 3,
            project_mode: "compact".to_owned(),
            maturity: "mvp".to_owned(),
            base_revision: 1,
            base_revision_id: Some(REVISION_ONE_ID.to_owned()),
            lifecycle: "proposed".to_owned(),
            schema_version: "forge.project-charter/v1".to_owned(),
            render_version: "forge.project-charter-render/v1".to_owned(),
            content_json: r#"{"success":{"acceptance_statements":["Changed"]}}"#.to_owned(),
            rendered_view: "# Changed Project".to_owned(),
            change_summary: "material scope change".to_owned(),
            author_type: "agent".to_owned(),
            author_id: Some(IDENTITY_ID.to_owned()),
            source_message_id: None,
            source_turn_job_id: None,
            source_refs_json: "[]".to_owned(),
            content_digest: "charter-content-2".to_owned(),
            rendered_digest: "charter-render-2".to_owned(),
            created_at: NOW.to_owned(),
            command_receipt: None,
            action_execution: None,
        },
    )
    .await
    .expect("amendment revision");

    sqlx::query(
        "INSERT INTO project_execution_baseline (
            id, project_id, current_revision_id, lifecycle, version, created_at, updated_at
         ) VALUES ('baseline-for-amendment', ?, NULL, 'draft', 1, ?, ?)",
    )
    .bind(PROJECT_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("baseline");
    sqlx::query(
        "INSERT INTO project_execution_baseline_revision (
            id, baseline_id, revision, base_revision, base_revision_id, lifecycle,
            charter_revision_id, document_revisions_json, plan_items_json,
            milestone_id, milestone_ids_json, milestone_definition_revision_ids_json,
            primary_milestone_id, release_policy_json, release_policy_revision,
            release_policy_digest, acceptance_matrix_json, capability_classes_json,
            risk_classes_json, adaptive_envelope_json, elevated_operations_json,
            exclusions_json, rollback_recovery_json, schema_version, render_version,
            rendered_view, content_digest, rendered_digest, source_refs_json, created_at
         ) VALUES ('baseline-revision-for-amendment', 'baseline-for-amendment', 1, 0,
                   NULL, 'approved', ?, '[]', '[]', NULL, '[]', '[]', NULL, '{}',
                   'release-policy@1', 'release-policy-digest', '[]', '[]', '[]',
                   '{}', '[]', '[]', '{}', 'baseline/v1', 'baseline-render/v1',
                   '# Baseline', 'baseline-content', 'baseline-render', '[]', ?)",
    )
    .bind(REVISION_ONE_ID)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("baseline revision");
    sqlx::query(
        "UPDATE project_execution_baseline
         SET current_revision_id = 'baseline-revision-for-amendment', lifecycle = 'active'
         WHERE id = 'baseline-for-amendment'",
    )
    .execute(db.pool())
    .await
    .expect("activate baseline");

    let amendment_outcome = serde_json::json!({
        "project_id": PROJECT_ID,
        "charter_id": CHARTER_ID,
        "revision_id": "charter-command-revision-2",
        "approval_id": "amendment-approval",
        "binding_id": "amendment-binding",
        "amendment_id": "amendment-record",
    });
    let amendment = ApplyProjectCharterApprovalCommand {
        approval: approval(
            "amendment-approval",
            "charter_amendment",
            "charter-command-revision-2",
            4,
            &skill_revision_id,
            "amendment-event",
        ),
        project_id: PROJECT_ID.to_owned(),
        expected_project_version: adopted.project_version,
        expected_current_charter_revision_id: Some(REVISION_ONE_ID.to_owned()),
        existing_binding_id: adopted.project_agent_binding_id.clone(),
        replacement_binding_id: Some("amendment-binding".to_owned()),
        bootstrap_message_id: None,
        bootstrap_content: None,
        bootstrap_content_guard_json: None,
        bootstrap_author_id: None,
        bootstrap_correlation_id: None,
        bootstrap_source_metadata_json: None,
        amendment_id: Some("amendment-record".to_owned()),
        amendment_rationale: Some("material scope change".to_owned()),
        amendment_material_diff_json: Some(r#"{"changed_sections":["success"]}"#.to_owned()),
        amendment_affected_records_json: Some(
            r#"{"reconciliation_required":["baselines","tasks"]}"#.to_owned(),
        ),
        command_receipt: Some(receipt("amendment-command", amendment_outcome)),
        action_execution: None,
    };
    let amended =
        ProjectOrchestrationRepo::apply_project_charter_approval_command(&db, amendment.clone())
            .await
            .expect("amendment");
    assert_eq!(
        amended.project_charter_revision_id,
        "charter-command-revision-2"
    );
    assert_eq!(amended.amendment_id.as_deref(), Some("amendment-record"));
    assert_eq!(amended.project_version, adopted.project_version + 1);
    let baseline_lifecycle: String = sqlx::query_scalar(
        "SELECT lifecycle FROM project_execution_baseline WHERE id = 'baseline-for-amendment'",
    )
    .fetch_one(db.pool())
    .await
    .expect("baseline lifecycle");
    let old_revision_lifecycle: String =
        sqlx::query_scalar("SELECT lifecycle FROM project_charter_revision WHERE id = ?")
            .bind(REVISION_ONE_ID)
            .fetch_one(db.pool())
            .await
            .expect("old revision lifecycle");
    assert_eq!(baseline_lifecycle, "superseded");
    assert_eq!(old_revision_lifecycle, "superseded");

    let replay =
        ProjectOrchestrationRepo::apply_project_charter_approval_command(&db, amendment.clone())
            .await
            .expect("amendment replay");
    assert_eq!(replay, amended);

    let mut changed = amendment;
    changed
        .command_receipt
        .as_mut()
        .expect("amendment receipt")
        .input_digest = "changed-digest".to_owned();
    let error = ProjectOrchestrationRepo::apply_project_charter_approval_command(&db, changed)
        .await
        .expect_err("changed replay conflicts");
    assert!(matches!(error, DbError::IdempotencyConflict));
    let receipt_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM command_receipt WHERE idempotency_key = 'amendment-command'",
    )
    .fetch_one(db.pool())
    .await
    .expect("amendment receipt");
    assert_eq!(receipt_count, 1);
}
