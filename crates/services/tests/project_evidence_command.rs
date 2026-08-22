//! Gate A acceptance coverage for the shared Project Evidence command.
//!
//! REST/user invocations and Project-Agent action execution intentionally use
//! different adapters in production.  These fixtures enter below those
//! adapters and assert that both paths persist the same Project-scoped command
//! identity and domain outcome shape, while only the action path links an
//! AgentAction execution.

use std::sync::Arc;

use db::{
    create_sqlite_pool, run_migrations, AgentActionPolicyResult, AgentActionRepo,
    AgentActionStatus, AgentRepo, AgentStatus, CreateAgentAction, CreateAgentIdentity,
    CreateAgentProfile, CreateProject, ProjectRepo, SqliteDb, User, UserRepo,
};
use forge_agent_host::PROJECT_EVIDENCE_OPERATION;
use serde_json::{json, Value};
use services::{
    ExecuteProjectOrchestrationActionInput, ProjectArtifactCommandService,
    ProjectCommandAuthorization, ProjectEvidenceCommand, ProjectOrchestrationActionService,
};
use sqlx::Row;

const ACCOUNT_ID: &str = "evidence-command-account";
const AGENT_ID: &str = "evidence-command-agent";
const PROFILE_ID: &str = "evidence-command-profile";
const PROJECT_ID: &str = "evidence-command-project";
const MILESTONE_ID: &str = "evidence-command-milestone";
const USER_ASSET_ID: &str = "evidence-command-user-asset";
const AGENT_ASSET_ID: &str = "evidence-command-agent-asset";
const CHECKSUM: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
const NOW: &str = "2026-08-20T00:00:00.000Z";

struct Fixture {
    db: Arc<SqliteDb>,
}

async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("SQLite pool creates");
    run_migrations(&pool).await.expect("migrations run");
    Arc::new(SqliteDb::new(pool))
}

async fn fixture() -> Fixture {
    let db = database().await;
    UserRepo::create_user(
        &*db,
        &User {
            id: ACCOUNT_ID.to_owned(),
            email: "evidence-command@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Evidence Command User".to_owned()),
            is_admin: false,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("user creates");
    AgentRepo::create_identity_with_profile(
        &*db,
        CreateAgentIdentity {
            id: AGENT_ID.to_owned(),
            name: "Evidence Project Agent".to_owned(),
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
            identity_id: AGENT_ID.to_owned(),
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
    .expect("Project Agent identity creates");
    ProjectRepo::create_with_agent_binding(
        &*db,
        CreateProject {
            id: PROJECT_ID.to_owned(),
            name: "Evidence Command Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(ACCOUNT_ID.to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        Some(AGENT_ID.to_owned()),
        Some(PROFILE_ID.to_owned()),
    )
    .await
    .expect("Project creates with active Project Agent binding");

    // Keep the fixture small: the command only needs a current milestone and
    // two available, checksum-bound media assets.  The domain command itself
    // validates all Project and mutable-version relationships.
    sqlx::query(
        "INSERT INTO project_milestone
            (id, project_id, milestone_sequence, milestone_key, lifecycle,
             created_at, updated_at)
         VALUES (?, ?, 1, 'M001', 'active', ?, ?)",
    )
    .bind(MILESTONE_ID)
    .bind(PROJECT_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("milestone creates");
    for asset_id in [USER_ASSET_ID, AGENT_ASSET_ID] {
        sqlx::query(
            "INSERT INTO media_asset
                (id, project_id, display_filename, content_type, byte_size,
                 storage_key, checksum, availability, gc_state, version,
                 created_at, updated_at)
             VALUES (?, ?, 'proof.png', 'image/png', 4, ?, ?, 'available',
                     'referenced', 1, ?, ?)",
        )
        .bind(asset_id)
        .bind(PROJECT_ID)
        .bind(format!("evidence-command/{asset_id}.png"))
        .bind(CHECKSUM)
        .bind(NOW)
        .bind(NOW)
        .execute(db.pool())
        .await
        .expect("media asset creates");
    }
    Fixture { db }
}

async fn arm_receipt_failpoint(db: &SqliteDb, trigger_name: &str, message: &str) {
    sqlx::query(&format!(
        "CREATE TEMP TRIGGER {trigger_name}
         BEFORE INSERT ON command_receipt
         BEGIN SELECT RAISE(ABORT, '{message}'); END"
    ))
    .execute(db.pool())
    .await
    .expect("command receipt failpoint creates");
}

async fn drop_receipt_failpoint(db: &SqliteDb, trigger_name: &str) {
    sqlx::query(&format!("DROP TRIGGER {trigger_name}"))
        .execute(db.pool())
        .await
        .expect("command receipt failpoint drops");
}

fn user_authorization(correlation_id: &str, principal_id: &str) -> ProjectCommandAuthorization {
    ProjectCommandAuthorization {
        principal_type: "user".to_owned(),
        principal_id: principal_id.to_owned(),
        policy_result: "allowed".to_owned(),
        policy_revision: Some("user-policy@1".to_owned()),
        policy_digest: Some("user-policy-digest".to_owned()),
        requested_permission: Some("project.evidence.attach".to_owned()),
        correlation_id: correlation_id.to_owned(),
        causation_id: None,
        causation_depth: 0,
        authorization_event_id: format!("authorization-{correlation_id}"),
        authorization_basis: "explicit authenticated user authorization".to_owned(),
        authorization_action: "project.evidence.attach".to_owned(),
        authorization_occurred_at: db::now_rfc3339(),
        authorization_json: json!({
            "principal": {"kind": "user", "id": principal_id},
            "action": "project.evidence.attach",
            "event_id": format!("authorization-{correlation_id}"),
        })
        .to_string(),
    }
}

fn user_command(key: &str, caption: &str, asset_id: &str) -> ProjectEvidenceCommand {
    ProjectEvidenceCommand {
        project_id: PROJECT_ID.to_owned(),
        milestone_id: MILESTONE_ID.to_owned(),
        asset_id: asset_id.to_owned(),
        task_id: None,
        source_run_id: None,
        source_validation_id: None,
        acceptance_check_ids: Vec::new(),
        caption: caption.to_owned(),
        evidence_kind: "screenshot".to_owned(),
        checksum: CHECKSUM.to_owned(),
        expected_milestone_version: 1,
        idempotency_key: key.to_owned(),
        authorization: user_authorization(&format!("correlation-{key}"), ACCOUNT_ID),
    }
}

fn assert_outcome_shape(outcome: &Value) {
    assert_eq!(outcome["operation"], PROJECT_EVIDENCE_OPERATION);
    assert_eq!(outcome["project_id"], PROJECT_ID);
    assert_eq!(outcome["milestone_id"], MILESTONE_ID);
    assert!(outcome["asset_id"].as_str().is_some());
    assert!(outcome["attachment_id"].as_str().is_some());
    assert!(outcome["domain_committed"].as_bool().unwrap_or(false));
}

#[tokio::test]
async fn direct_user_and_native_agent_evidence_share_project_receipt_shape() {
    let fixture = fixture().await;
    let direct = ProjectArtifactCommandService::new(Arc::clone(&fixture.db));
    let user_attachment = direct
        .attach_evidence(
            user_command("user-evidence-key", "User proof", USER_ASSET_ID),
            None,
        )
        .await
        .expect("direct authenticated user evidence command");
    assert_eq!(user_attachment.project_id, PROJECT_ID);
    assert_eq!(user_attachment.author_type, "user");

    let user_receipt = sqlx::query(
        "SELECT principal_type, principal_id, scope_type, scope_id, operation,
                event_id, agent_action_execution_id, outcome_json
         FROM command_receipt
         WHERE idempotency_key = 'user-evidence-key'",
    )
    .fetch_one(fixture.db.pool())
    .await
    .expect("direct command receipt");
    assert_eq!(user_receipt.get::<String, _>("principal_type"), "user");
    assert_eq!(user_receipt.get::<String, _>("principal_id"), ACCOUNT_ID);
    assert_eq!(user_receipt.get::<String, _>("scope_type"), "project");
    assert_eq!(user_receipt.get::<String, _>("scope_id"), PROJECT_ID);
    assert_eq!(
        user_receipt.get::<String, _>("operation"),
        PROJECT_EVIDENCE_OPERATION
    );
    assert!(user_receipt
        .get::<Option<String>, _>("agent_action_execution_id")
        .is_none());
    let user_outcome: Value = serde_json::from_str(&user_receipt.get::<String, _>("outcome_json"))
        .expect("direct receipt outcome JSON");
    assert_outcome_shape(&user_outcome);
    assert_eq!(user_outcome["attachment_id"], user_attachment.id);
    let user_event = sqlx::query(
        "SELECT event_type, actor_type, actor_id, scope_type, scope_id, correlation_id
         FROM domain_event WHERE id = ?",
    )
    .bind(user_receipt.get::<String, _>("event_id"))
    .fetch_one(fixture.db.pool())
    .await
    .expect("direct evidence event");
    assert_eq!(
        user_event.get::<String, _>("event_type"),
        "project.evidence.attached"
    );
    assert_eq!(user_event.get::<String, _>("actor_type"), "user");
    assert_eq!(user_event.get::<String, _>("actor_id"), ACCOUNT_ID);
    assert_eq!(user_event.get::<String, _>("scope_type"), "project");
    assert_eq!(user_event.get::<String, _>("scope_id"), PROJECT_ID);

    let action_payload = json!({
        "milestone_id": MILESTONE_ID,
        "asset_id": AGENT_ASSET_ID,
        "checksum": CHECKSUM,
        "acceptance_check_ids": [],
        "caption": "Agent proof",
        "kind": "screenshot",
        "expected_milestone_version": 1,
    });
    let action = AgentActionRepo::create_action(
        &*fixture.db,
        CreateAgentAction {
            id: "evidence-command-action".to_owned(),
            actor_identity_id: AGENT_ID.to_owned(),
            scope_type: "project".to_owned(),
            scope_id: PROJECT_ID.to_owned(),
            operation: PROJECT_EVIDENCE_OPERATION.to_owned(),
            payload_json: action_payload.to_string(),
            payload_hash: "evidence-command-action-payload".to_owned(),
            dedupe_key: "evidence-command-action-dedupe".to_owned(),
            correlation_id: "agent-evidence-correlation".to_owned(),
            causation_id: None,
            causation_depth: 0,
            requested_permission: "project.evidence.attach".to_owned(),
            policy_result: AgentActionPolicyResult::Allowed,
            policy_reason: None,
            status: AgentActionStatus::Proposed,
            target_type: Some("project".to_owned()),
            target_id: Some(PROJECT_ID.to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("Project Agent evidence action creates");
    let action_service = ProjectOrchestrationActionService::new(Arc::clone(&fixture.db));
    let execution_input = ExecuteProjectOrchestrationActionInput {
        action_id: action.id.clone(),
        expected_version: action.version,
        executed_by_type: "agent".to_owned(),
        executed_by_id: AGENT_ID.to_owned(),
        idempotency_key: "agent-evidence-key".to_owned(),
    };
    let execution = action_service
        .execute(execution_input.clone())
        .await
        .expect("action-backed Project Agent evidence command");
    let agent_receipt = sqlx::query(
        "SELECT principal_type, principal_id, scope_type, scope_id, operation,
                event_id, agent_action_execution_id, outcome_json
         FROM command_receipt
         WHERE idempotency_key = 'agent-evidence-key'",
    )
    .fetch_one(fixture.db.pool())
    .await
    .expect("native command receipt");
    assert_eq!(agent_receipt.get::<String, _>("principal_type"), "agent");
    assert_eq!(agent_receipt.get::<String, _>("principal_id"), AGENT_ID);
    assert_eq!(agent_receipt.get::<String, _>("scope_type"), "project");
    assert_eq!(agent_receipt.get::<String, _>("scope_id"), PROJECT_ID);
    assert_eq!(
        agent_receipt.get::<String, _>("operation"),
        PROJECT_EVIDENCE_OPERATION
    );
    assert_eq!(
        agent_receipt.get::<String, _>("agent_action_execution_id"),
        execution.id
    );
    let agent_outcome: Value =
        serde_json::from_str(&agent_receipt.get::<String, _>("outcome_json"))
            .expect("native receipt outcome JSON");
    assert_outcome_shape(&agent_outcome);
    assert!(agent_outcome["attachment_id"].as_str().is_some());
    assert_eq!(agent_outcome["asset_id"], AGENT_ASSET_ID);
    assert_eq!(
        execution
            .result_json
            .as_deref()
            .map(|value| serde_json::from_str::<Value>(value).unwrap()),
        Some(agent_outcome.clone())
    );
    let agent_event = sqlx::query(
        "SELECT event_type, actor_type, actor_id, scope_type, scope_id, correlation_id
         FROM domain_event WHERE id = ?",
    )
    .bind(agent_receipt.get::<String, _>("event_id"))
    .fetch_one(fixture.db.pool())
    .await
    .expect("native evidence event");
    assert_eq!(
        agent_event.get::<String, _>("event_type"),
        "project.evidence.attached"
    );
    assert_eq!(agent_event.get::<String, _>("actor_type"), "agent");
    assert_eq!(agent_event.get::<String, _>("actor_id"), AGENT_ID);
    assert_eq!(agent_event.get::<String, _>("scope_type"), "project");
    assert_eq!(agent_event.get::<String, _>("scope_id"), PROJECT_ID);
    assert_eq!(
        agent_event.get::<String, _>("correlation_id"),
        "agent-evidence-correlation"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_action_execution WHERE action_id = ?",
        )
        .bind(&action.id)
        .fetch_one(fixture.db.pool())
        .await
        .expect("native action execution count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_media_attachment
             WHERE project_id = ? AND attachment_kind = 'evidence'",
        )
        .bind(PROJECT_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("evidence attachment count"),
        2
    );
}

#[tokio::test]
async fn evidence_replay_is_exact_across_mutable_state_and_conflicts_are_fail_closed() {
    let fixture = fixture().await;
    let service = ProjectArtifactCommandService::new(Arc::clone(&fixture.db));
    let command = user_command("evidence-replay-key", "Frozen proof", USER_ASSET_ID);
    let first = service
        .attach_evidence(command.clone(), None)
        .await
        .expect("first evidence command");

    // Receipt resolution precedes mutable validation.  Both the milestone
    // version and unrelated active-binding settings may change after a
    // response is lost without creating a second attachment.
    sqlx::query("UPDATE project_milestone SET version = 9 WHERE id = ?")
        .bind(MILESTONE_ID)
        .execute(fixture.db.pool())
        .await
        .expect("milestone advances");
    sqlx::query(
        "UPDATE project_agent_binding
         SET autonomy_policy_json = '{\"changed\":true}'
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(PROJECT_ID)
    .execute(fixture.db.pool())
    .await
    .expect("binding settings change");
    let replay = service
        .attach_evidence(command.clone(), None)
        .await
        .expect("exact response-loss replay");
    assert_eq!(replay, first);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_media_attachment
             WHERE project_id = ? AND asset_id = ? AND attachment_kind = 'evidence'",
        )
        .bind(PROJECT_ID)
        .bind(USER_ASSET_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("replay attachment count"),
        1
    );

    let mut changed_digest = command.clone();
    changed_digest.caption = "Changed proof".to_owned();
    let digest_conflict = service.attach_evidence(changed_digest, None).await;
    assert!(
        matches!(
            digest_conflict,
            Err(services::ServiceError::Db(db::DbError::IdempotencyConflict))
        ),
        "changed command input must be an idempotency conflict: {digest_conflict:?}"
    );

    let mut changed_principal = command;
    changed_principal.authorization =
        user_authorization("correlation-evidence-replay-key", "other-user");
    let principal_conflict = service.attach_evidence(changed_principal, None).await;
    assert!(
        principal_conflict.is_err(),
        "changed principal must fail closed"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_media_attachment
             WHERE project_id = ? AND asset_id = ? AND attachment_kind = 'evidence'",
        )
        .bind(PROJECT_ID)
        .bind(USER_ASSET_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("principal conflict attachment count"),
        1
    );
}

#[tokio::test]
async fn native_evidence_requires_expected_milestone_version_and_rejects_stale_cas() {
    let fixture = fixture().await;
    let action_payload = json!({
        "milestone_id": MILESTONE_ID,
        "asset_id": AGENT_ASSET_ID,
        "checksum": CHECKSUM,
        "acceptance_check_ids": [],
        "caption": "Agent proof",
        "kind": "screenshot",
        "expected_milestone_version": 1,
    });
    let action = AgentActionRepo::create_action(
        &*fixture.db,
        CreateAgentAction {
            id: "evidence-stale-action".to_owned(),
            actor_identity_id: AGENT_ID.to_owned(),
            scope_type: "project".to_owned(),
            scope_id: PROJECT_ID.to_owned(),
            operation: PROJECT_EVIDENCE_OPERATION.to_owned(),
            payload_json: action_payload.to_string(),
            payload_hash: "evidence-stale-action-payload".to_owned(),
            dedupe_key: "evidence-stale-action-dedupe".to_owned(),
            correlation_id: "stale-evidence-correlation".to_owned(),
            causation_id: None,
            causation_depth: 0,
            requested_permission: "project.evidence.attach".to_owned(),
            policy_result: AgentActionPolicyResult::Allowed,
            policy_reason: None,
            status: AgentActionStatus::Proposed,
            target_type: Some("project".to_owned()),
            target_id: Some(PROJECT_ID.to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("stale Project Agent evidence action creates");
    sqlx::query("UPDATE project_milestone SET version = 2 WHERE id = ?")
        .bind(MILESTONE_ID)
        .execute(fixture.db.pool())
        .await
        .expect("milestone advances before native execution");

    let result = ProjectOrchestrationActionService::new(Arc::clone(&fixture.db))
        .execute(ExecuteProjectOrchestrationActionInput {
            action_id: action.id,
            expected_version: action.version,
            executed_by_type: "agent".to_owned(),
            executed_by_id: AGENT_ID.to_owned(),
            idempotency_key: "evidence-stale-execution".to_owned(),
        })
        .await;
    assert!(
        matches!(
            result,
            Err(services::ServiceError::Db(db::DbError::VersionConflict))
        ),
        "stale native evidence must fail milestone CAS: {result:?}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_media_attachment
             WHERE project_id = ? AND asset_id = ? AND attachment_kind = 'evidence'",
        )
        .bind(PROJECT_ID)
        .bind(AGENT_ASSET_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("stale native evidence attachment count"),
        0
    );
}

#[tokio::test]
async fn evidence_receipt_failpoint_rolls_back_attachment_and_media_state() {
    let fixture = fixture().await;
    let command = user_command("failpoint-evidence", "Failpoint proof", USER_ASSET_ID);
    let before_milestone: i64 =
        sqlx::query_scalar("SELECT version FROM project_milestone WHERE id = ? AND project_id = ?")
            .bind(MILESTONE_ID)
            .bind(PROJECT_ID)
            .fetch_one(fixture.db.pool())
            .await
            .expect("evidence milestone pre-state");
    let before_media: (i64, String, String, i64) = sqlx::query_as(
        "SELECT COUNT(*), availability, gc_state, version
         FROM media_asset WHERE id = ? AND project_id = ?",
    )
    .bind(USER_ASSET_ID)
    .bind(PROJECT_ID)
    .fetch_one(fixture.db.pool())
    .await
    .expect("evidence media pre-state");
    assert_eq!(before_milestone, 1);
    assert_eq!(
        before_media,
        (1, "available".to_owned(), "referenced".to_owned(), 1)
    );

    arm_receipt_failpoint(
        &fixture.db,
        "evidence_receipt_failpoint",
        "evidence receipt failpoint",
    )
    .await;
    let failed = ProjectArtifactCommandService::new(Arc::clone(&fixture.db))
        .attach_evidence(command.clone(), None)
        .await
        .expect_err("receipt failpoint aborts evidence attach");
    assert!(failed.to_string().contains("failpoint"));
    drop_receipt_failpoint(&fixture.db, "evidence_receipt_failpoint").await;

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_media_attachment
             WHERE project_id = ? AND attachment_kind = 'evidence'",
        )
        .bind(PROJECT_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("evidence attachment absence"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE scope_id = ? AND operation = ? AND idempotency_key = ?",
        )
        .bind(PROJECT_ID)
        .bind(PROJECT_EVIDENCE_OPERATION)
        .bind("failpoint-evidence")
        .fetch_one(fixture.db.pool())
        .await
        .expect("evidence receipt absence"),
        0
    );
    let after_failure_milestone: i64 =
        sqlx::query_scalar("SELECT version FROM project_milestone WHERE id = ? AND project_id = ?")
            .bind(MILESTONE_ID)
            .bind(PROJECT_ID)
            .fetch_one(fixture.db.pool())
            .await
            .expect("evidence milestone post-failure");
    let after_failure_media: (i64, String, String, i64) = sqlx::query_as(
        "SELECT COUNT(*), availability, gc_state, version
         FROM media_asset WHERE id = ? AND project_id = ?",
    )
    .bind(USER_ASSET_ID)
    .bind(PROJECT_ID)
    .fetch_one(fixture.db.pool())
    .await
    .expect("evidence media post-failure");
    assert_eq!(after_failure_milestone, before_milestone);
    assert_eq!(after_failure_media, before_media);

    let first = ProjectArtifactCommandService::new(Arc::clone(&fixture.db))
        .attach_evidence(command.clone(), None)
        .await
        .expect("evidence retry after receipt failpoint");
    assert_eq!(first.project_id, PROJECT_ID);
    assert_eq!(first.milestone_id.as_deref(), Some(MILESTONE_ID));
    assert_eq!(first.asset_id, USER_ASSET_ID);
    assert_eq!(first.attachment_kind, "evidence");
    assert_eq!(first.availability, "available");
    assert_eq!(first.checksum.as_deref(), Some(CHECKSUM));
    assert_eq!(first.author_type, "user");
    let media_after_first: (String, String, i64) = sqlx::query_as(
        "SELECT availability, gc_state, version
         FROM media_asset WHERE id = ? AND project_id = ?",
    )
    .bind(USER_ASSET_ID)
    .bind(PROJECT_ID)
    .fetch_one(fixture.db.pool())
    .await
    .expect("evidence media after successful attach");
    assert_eq!(
        media_after_first.0, "available",
        "successful evidence keeps the asset available"
    );
    assert_eq!(media_after_first.1, "referenced");
    assert!(
        media_after_first.2 > before_media.3,
        "successful evidence records the media reference transition"
    );

    sqlx::query("UPDATE project_milestone SET version = 9 WHERE id = ?")
        .bind(MILESTONE_ID)
        .execute(fixture.db.pool())
        .await
        .expect("mutate evidence milestone after commit");
    let replay = ProjectArtifactCommandService::new(Arc::clone(&fixture.db))
        .attach_evidence(command, None)
        .await
        .expect("evidence replay after service recreation");
    assert_eq!(replay, first);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_media_attachment
             WHERE project_id = ? AND asset_id = ? AND attachment_kind = 'evidence'",
        )
        .bind(PROJECT_ID)
        .bind(USER_ASSET_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("evidence attachment count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type = 'project.evidence.attached'",
        )
        .bind(PROJECT_ID)
        .fetch_one(fixture.db.pool())
        .await
        .expect("evidence event count"),
        1
    );
    let media_after_success: (String, String, i64) = sqlx::query_as(
        "SELECT availability, gc_state, version
         FROM media_asset WHERE id = ? AND project_id = ?",
    )
    .bind(USER_ASSET_ID)
    .bind(PROJECT_ID)
    .fetch_one(fixture.db.pool())
    .await
    .expect("evidence media post-success");
    assert_eq!(media_after_success, media_after_first);
}
