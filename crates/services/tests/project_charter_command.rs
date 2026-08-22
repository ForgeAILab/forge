//! Gate A acceptance coverage for the Project Charter adoption/amendment
//! command boundary.
//!
//! These tests enter the transport-neutral Project Charter command service and
//! assert the DB composite's durable event/receipt/action guarantees.

use std::sync::Arc;

use api_types::{PrincipalKind, PrincipalRef, ProjectCharterContent, RevisionProvenance};
use db::{
    create_sqlite_pool, run_migrations, AgentActionPolicyResult, AgentActionRepo,
    AgentActionStatus, AgentRepo, AgentStatus, CreateAgentAction, CreateAgentIdentity,
    CreateAgentProfile, CreateProject, CreateProjectCharter, CreateProjectCharterRevision,
    CreateProjectCharterRevisionAtomically, ProjectOrchestrationRepo, ProjectRepo, SqliteDb, User,
    UserRepo,
};
use services::{
    render_and_digest_charter, AgentActionProvenance, ProjectCharterApprovalCommand,
    ProjectCharterCommandService, ProjectCharterRevisionCommand, ProjectCommandAuthorization,
    PROJECT_CHARTER_APPROVAL_COMMAND,
};
use sha2::{Digest, Sha256};

const ACCOUNT_ID: &str = "charter-service-command-user";
const PROJECT_ID: &str = "charter-service-command-project";
const IDENTITY_ID: &str = "charter-service-command-agent";
const PROFILE_ID: &str = "charter-service-command-profile";
const CHARTER_ID: &str = "charter-service-command-charter";
const REVISION_ONE_ID: &str = "charter-service-command-revision-1";
const NOW: &str = "2026-08-20T00:00:00.000Z";
const PROJECT_SKILL_KEY: &str = "forge.project.orchestration/v1";
const PROJECT_AGENT_TOOL_POLICY: &str = r#"{"permissions":["read_project","propose_project"]}"#;

async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("SQLite pool creates");
    run_migrations(&pool).await.expect("migrations run");
    Arc::new(SqliteDb::new(pool))
}

fn policy_digest(policy: &str) -> String {
    let mut bytes = b"forge.project-agent-policy/v1\0".to_vec();
    bytes.extend_from_slice(policy.as_bytes());
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn charter_content(acceptance: &str) -> ProjectCharterContent {
    serde_json::from_value(serde_json::json!({
        "identity": {
            "working_name": "Adopted Charter Project",
            "slug_proposal": "adopted-charter-project",
            "one_line_vision": "Keep a Project outcome durable and auditable.",
            "maturity": "mvp",
            "lifecycle_intent": "validate the Charter command boundary",
            "project_type": "product",
            "value_proposition": "Make Charter adoption and amendment replay-safe."
        },
        "problem_and_people": {
            "problem_or_opportunity": "A lost command response must not duplicate a Charter mutation.",
            "target_users": ["Forge users"],
            "beneficiaries": ["Project collaborators"],
            "jobs_pains_opportunity": ["Continue from an approved Charter."],
            "current_alternatives": ["Manual handoff"],
            "stakeholders": ["Project owner"],
            "excluded_audiences": ["Unrelated accounts"]
        },
        "core_experience": {
            "primary_outcome": "One approved command governs one Project handoff.",
            "core_loop": "approve, amend, replay",
            "principal_journeys": ["User retries after response loss"]
        },
        "scope": {
            "must_have_outcomes": ["Persist the exact approved Charter."],
            "required_deliverables": ["One durable Project Chat."],
            "later_possibilities": ["Project-local planning"],
            "explicit_non_goals": ["Managing another account"]
        },
        "success": {
            "qualitative_outcome": "The handoff is exact.",
            "success_signals": ["Replay returns the same identifiers."],
            "acceptance_statements": [acceptance],
            "required_evidence": ["Durable receipt and event."],
            "non_claims": ["This does not prove implementation quality."]
        },
        "constraints_and_risks": {
            "product": ["Local-first single-user operation."],
            "time_and_budget": [],
            "technology": ["SQLite"],
            "data": ["Do not copy hidden Main Chat history"],
            "integrations": [],
            "security_privacy_compliance": ["Require explicit approval"],
            "accessibility": [],
            "operations": [],
            "migration": [],
            "launch": [],
            "agent_authority": ["Project Agent remains Project-scoped"],
            "risks": []
        },
        "knowledge_ledger": {"items": []},
        "handoff_note": {
            "recommended_first_action": "Validate the approved outcome.",
            "bounded_summary": "Continue from the exact Charter.",
            "unresolved_item_ids": []
        }
    }))
    .expect("valid Charter content")
}

async fn fixture() -> (Arc<SqliteDb>, String) {
    let db = database().await;
    UserRepo::create_user(
        &*db,
        &User {
            id: ACCOUNT_ID.to_owned(),
            email: "charter-service-command@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Charter command user".to_owned()),
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
            account_permission_ceiling: r#"{"permissions":["read_project","propose_project"]}"#
                .to_owned(),
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
            tool_policy_json: PROJECT_AGENT_TOOL_POLICY.to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("Project Agent creates");
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: PROJECT_ID.to_owned(),
            name: "Legacy Charter Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(ACCOUNT_ID.to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("Project creates");

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
    let initial_content = charter_content("A replay returns the same identifiers.");
    let initial_render = render_and_digest_charter(&initial_content);

    ProjectOrchestrationRepo::create_project_charter_revision_atomically(
        &*db,
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
                render_version: initial_render.render_version,
                content_json: serde_json::to_string(&initial_content).expect("Charter JSON"),
                rendered_view: initial_render.rendered_view,
                change_summary: "initial Charter".to_owned(),
                author_type: "agent".to_owned(),
                author_id: Some(IDENTITY_ID.to_owned()),
                source_message_id: None,
                source_turn_job_id: None,
                source_refs_json: "[]".to_owned(),
                content_digest: initial_render.content_digest,
                rendered_digest: initial_render.render_digest,
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

fn authorization(
    principal_type: &str,
    principal_id: &str,
    action: &str,
    event_id: &str,
    correlation_id: &str,
) -> ProjectCommandAuthorization {
    ProjectCommandAuthorization {
        principal_type: principal_type.to_owned(),
        principal_id: principal_id.to_owned(),
        policy_result: "allowed".to_owned(),
        policy_revision: Some("forge.project-agent-policy/v1".to_owned()),
        policy_digest: Some(policy_digest("{}")),
        requested_permission: Some("project.charter.approval".to_owned()),
        correlation_id: correlation_id.to_owned(),
        causation_id: None,
        causation_depth: 0,
        authorization_event_id: event_id.to_owned(),
        authorization_basis: "explicit authenticated authorization".to_owned(),
        authorization_action: action.to_owned(),
        authorization_occurred_at: NOW.to_owned(),
        authorization_json: "{}".to_owned(),
    }
}

#[tokio::test]
async fn charter_draft_receipt_failpoint_rolls_back_then_replays_after_service_recreation() {
    let (db, _skill_revision_id) = fixture().await;
    let initial_content = charter_content("A replay returns the same identifiers.");
    let initial_render = render_and_digest_charter(&initial_content);
    let amendment_content = charter_content("The draft amendment remains replay-safe.");
    let amendment_render = render_and_digest_charter(&amendment_content);
    let command = ProjectCharterRevisionCommand {
        project_id: PROJECT_ID.to_owned(),
        charter_id: CHARTER_ID.to_owned(),
        base_revision_id: Some(REVISION_ONE_ID.to_owned()),
        expected_digest: Some(initial_render.content_digest),
        project_mode: "compact".to_owned(),
        maturity: "mvp".to_owned(),
        content: amendment_content,
        rendered_view: Some(amendment_render.rendered_view),
        render_version: Some(amendment_render.render_version),
        provenance: RevisionProvenance {
            author: PrincipalRef {
                kind: PrincipalKind::User,
                id: ACCOUNT_ID.to_owned(),
                display_name: None,
            },
            profile_revision: None,
            operating_skill_revision: None,
            source_refs: Vec::new(),
            change_summary: "draft amendment".to_owned(),
            material_diff: None,
        },
        expected_charter_version: 2,
        idempotency_key: "charter-service-draft-failpoint".to_owned(),
        authorization: authorization(
            "user",
            ACCOUNT_ID,
            "project_charter.revision.save",
            "charter-service-draft-failpoint-event",
            "charter-service-draft-failpoint-correlation",
        ),
    };

    sqlx::query(
        "CREATE TEMP TRIGGER charter_draft_receipt_failpoint
         BEFORE INSERT ON command_receipt
         WHEN NEW.operation = 'project.charter.adoption'
         BEGIN SELECT RAISE(ABORT, 'charter draft receipt failpoint'); END;",
    )
    .execute(db.pool())
    .await
    .expect("charter draft receipt failpoint");

    let service = ProjectCharterCommandService::new(Arc::clone(&db));
    let stopped = service
        .save_revision(command.clone(), None)
        .await
        .expect_err("receipt failpoint stops the draft command");
    assert!(
        stopped
            .to_string()
            .contains("charter draft receipt failpoint"),
        "unexpected charter draft failpoint error: {stopped}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_charter_revision WHERE charter_id = ?",
        )
        .bind(CHARTER_ID)
        .fetch_one(db.pool())
        .await
        .expect("Charter revision count after rollback"),
        1,
        "the failed draft leaves no revision behind"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_charter_revision
             WHERE charter_id = ? AND base_revision_id = ?",
        )
        .bind(CHARTER_ID)
        .bind(REVISION_ONE_ID)
        .fetch_one(db.pool())
        .await
        .expect("draft amendment count after rollback"),
        0,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE operation = 'project.charter.adoption'
               AND idempotency_key = ?",
        )
        .bind("charter-service-draft-failpoint")
        .fetch_one(db.pool())
        .await
        .expect("draft receipt count after rollback"),
        0,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'project_charter.revision_created'
               AND correlation_id = ?",
        )
        .bind("charter-service-draft-failpoint-correlation")
        .fetch_one(db.pool())
        .await
        .expect("draft event count after rollback"),
        0,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT version FROM project_charter WHERE id = ?")
            .bind(CHARTER_ID)
            .fetch_one(db.pool())
            .await
            .expect("Charter version after rollback"),
        2,
        "the Charter CAS rolls back with the receipt"
    );

    sqlx::query("DROP TRIGGER charter_draft_receipt_failpoint")
        .execute(db.pool())
        .await
        .expect("remove charter draft receipt failpoint");

    // A new service instance represents the process that received no response
    // from the failed attempt.  The command itself is unchanged, so the
    // successful retry is the one server-minted revision that becomes frozen.
    drop(service);
    let retry_service = ProjectCharterCommandService::new(Arc::clone(&db));
    let first = retry_service
        .save_revision(command.clone(), None)
        .await
        .expect("draft retry after rollback");
    assert_eq!(first.revision.revision, 2);
    assert_eq!(first.charter_version, 3);
    assert_ne!(first.revision.id, REVISION_ONE_ID);

    drop(retry_service);
    let replay_service = ProjectCharterCommandService::new(Arc::clone(&db));
    let replay = replay_service
        .save_revision(command, None)
        .await
        .expect("draft replay after service recreation");
    assert_eq!(replay, first, "replay returns the frozen revision identity");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_charter_revision WHERE charter_id = ?",
        )
        .bind(CHARTER_ID)
        .fetch_one(db.pool())
        .await
        .expect("final Charter revision count"),
        2,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE operation = 'project.charter.adoption'
               AND idempotency_key = ?",
        )
        .bind("charter-service-draft-failpoint")
        .fetch_one(db.pool())
        .await
        .expect("final draft receipt count"),
        1,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'project_charter.revision_created'
               AND correlation_id = ?",
        )
        .bind("charter-service-draft-failpoint-correlation")
        .fetch_one(db.pool())
        .await
        .expect("final draft event count"),
        1,
    );
}

#[tokio::test]
async fn charter_adoption_receipt_failpoint_rolls_back_then_replays_after_service_recreation() {
    let (db, skill_revision_id) = fixture().await;
    let initial_content = charter_content("A replay returns the same identifiers.");
    let initial_render = render_and_digest_charter(&initial_content);
    let command = ProjectCharterApprovalCommand {
        project_id: PROJECT_ID.to_owned(),
        charter_id: CHARTER_ID.to_owned(),
        revision_id: REVISION_ONE_ID.to_owned(),
        content_digest: initial_render.content_digest,
        rendered_digest: initial_render.render_digest,
        expected_charter_version: 2,
        expected_project_version: 1,
        approved_project_name: "Adopted Charter Project".to_owned(),
        approved_project_slug: Some("adopted-charter-project".to_owned()),
        project_mode: "compact".to_owned(),
        selected_project_agent_identity_id: IDENTITY_ID.to_owned(),
        selected_project_agent_profile_revision_id: PROFILE_ID.to_owned(),
        selected_project_agent_operating_skill_revision: skill_revision_id,
        selected_project_agent_policy_digest: policy_digest(PROJECT_AGENT_TOOL_POLICY),
        idempotency_key: "charter-service-adoption-failpoint".to_owned(),
        authorization: authorization(
            "user",
            ACCOUNT_ID,
            "project_charter.approval",
            "charter-service-adoption-failpoint-event",
            "charter-service-adoption-failpoint-correlation",
        ),
    };

    sqlx::query(
        "CREATE TEMP TRIGGER charter_adoption_receipt_failpoint
         BEFORE INSERT ON command_receipt
         WHEN NEW.operation = 'project.charter.approval'
         BEGIN SELECT RAISE(ABORT, 'charter adoption receipt failpoint'); END;",
    )
    .execute(db.pool())
    .await
    .expect("charter adoption receipt failpoint");

    let service = ProjectCharterCommandService::new(Arc::clone(&db));
    let stopped = service
        .approve(command.clone(), None)
        .await
        .expect_err("receipt failpoint stops Charter adoption");
    assert!(
        stopped
            .to_string()
            .contains("charter adoption receipt failpoint"),
        "unexpected Charter adoption failpoint error: {stopped}"
    );
    for (sql, message) in [
        (
            "SELECT COUNT(*) FROM project_charter_approval
             WHERE idempotency_key = 'charter-service-adoption-failpoint'",
            "approval",
        ),
        (
            "SELECT COUNT(*) FROM project_charter_approval_event
             WHERE idempotency_key LIKE 'charter-service-adoption-failpoint:%'",
            "approval lifecycle events",
        ),
        (
            "SELECT COUNT(*) FROM agent_chat_message
             WHERE source_type = 'native' AND source_id IN
               (SELECT id FROM project_charter_approval
                WHERE idempotency_key = 'charter-service-adoption-failpoint')",
            "bootstrap message",
        ),
        (
            "SELECT COUNT(*) FROM command_receipt
             WHERE operation = 'project.charter.approval'
               AND idempotency_key = 'charter-service-adoption-failpoint'",
            "command receipt",
        ),
        (
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'project.charter.approved'
               AND correlation_id = 'charter-service-adoption-failpoint-correlation'",
            "approval event",
        ),
    ] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>(sql)
                .fetch_one(db.pool())
                .await
                .expect(message),
            0,
            "{message} must roll back with the receipt"
        );
    }
    let project_state: (String, i64, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT charter_status, version, current_charter_id,
                current_charter_revision_id
         FROM project WHERE id = ?",
    )
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("Project state after adoption rollback");
    assert_eq!(project_state.0, "legacy_unverified");
    assert_eq!(project_state.1, 1);
    assert!(project_state.2.is_none());
    assert!(project_state.3.is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_chat_message
             WHERE chat_id = (SELECT id FROM agent_chat
                              WHERE kind = 'project' AND project_id = ?)",
        )
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("Project Chat message count after rollback"),
        0,
    );

    sqlx::query("DROP TRIGGER charter_adoption_receipt_failpoint")
        .execute(db.pool())
        .await
        .expect("remove charter adoption receipt failpoint");
    drop(service);
    let retry_service = ProjectCharterCommandService::new(Arc::clone(&db));
    let first = retry_service
        .approve(command.clone(), None)
        .await
        .expect("Charter adoption retry after rollback");
    assert_eq!(first.approval.lifecycle, "consumed");
    assert!(first.bootstrap_message_id.is_some());

    drop(retry_service);
    let replay_service = ProjectCharterCommandService::new(Arc::clone(&db));
    let replay = replay_service
        .approve(command, None)
        .await
        .expect("Charter adoption replay after service recreation");
    assert_eq!(replay, first, "replay returns all frozen adoption IDs");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_charter_approval
             WHERE idempotency_key = 'charter-service-adoption-failpoint'",
        )
        .fetch_one(db.pool())
        .await
        .expect("final Charter approval count"),
        1,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_charter_approval_event
             WHERE idempotency_key LIKE 'charter-service-adoption-failpoint:%'",
        )
        .fetch_one(db.pool())
        .await
        .expect("final Charter approval event count"),
        2,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'project.charter.approved'
               AND correlation_id = 'charter-service-adoption-failpoint-correlation'",
        )
        .fetch_one(db.pool())
        .await
        .expect("final Charter approval domain event count"),
        1,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_chat_message
             WHERE chat_id = (SELECT id FROM agent_chat
                              WHERE kind = 'project' AND project_id = ?)",
        )
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("final Project Chat message count"),
        1,
    );
}

#[tokio::test]
async fn charter_user_adoption_and_action_backed_amendment_are_atomic_replay_safe() {
    let (db, skill_revision_id) = fixture().await;
    let service = ProjectCharterCommandService::new(Arc::clone(&db));
    let initial_content = charter_content("A replay returns the same identifiers.");
    let initial_render = render_and_digest_charter(&initial_content);

    let adoption = ProjectCharterApprovalCommand {
        project_id: PROJECT_ID.to_owned(),
        charter_id: CHARTER_ID.to_owned(),
        revision_id: REVISION_ONE_ID.to_owned(),
        content_digest: initial_render.content_digest.clone(),
        rendered_digest: initial_render.render_digest.clone(),
        expected_charter_version: 2,
        expected_project_version: 1,
        approved_project_name: "Adopted Charter Project".to_owned(),
        approved_project_slug: Some("adopted-charter-project".to_owned()),
        project_mode: "compact".to_owned(),
        selected_project_agent_identity_id: IDENTITY_ID.to_owned(),
        selected_project_agent_profile_revision_id: PROFILE_ID.to_owned(),
        selected_project_agent_operating_skill_revision: skill_revision_id.clone(),
        selected_project_agent_policy_digest: policy_digest(PROJECT_AGENT_TOOL_POLICY),
        idempotency_key: "charter-service-adoption-command".to_owned(),
        authorization: authorization(
            "user",
            ACCOUNT_ID,
            "project_charter.approval",
            "charter-service-adoption-event",
            "charter-service-adoption-correlation",
        ),
    };
    let adopted = service
        .approve(adoption.clone(), None)
        .await
        .expect("service user adoption");
    assert_eq!(adopted.project_version, 2);
    assert_eq!(adopted.approval.lifecycle, "consumed");
    assert!(adopted.bootstrap_message_id.is_some());
    let bootstrap_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_chat_message WHERE id = ? AND source_id = ?",
    )
    .bind(adopted.bootstrap_message_id.as_deref())
    .bind(&adopted.approval.id)
    .fetch_one(db.pool())
    .await
    .expect("adoption bootstrap message");
    assert_eq!(bootstrap_count, 1);

    sqlx::query(
        r#"UPDATE project_agent_binding
         SET autonomy_policy_json = '{"changed":true}'
         WHERE project_id = ? AND state = 'active'"#,
    )
    .bind(PROJECT_ID)
    .execute(db.pool())
    .await
    .expect("binding policy changes");
    let adoption_replay = service
        .approve(adoption.clone(), None)
        .await
        .expect("exact adoption replay after mutable binding change");
    assert_eq!(adoption_replay, adopted);

    let mut changed_principal = adoption.clone();
    changed_principal.authorization = authorization(
        "user",
        "another-user",
        "project_charter.approval",
        "charter-service-adoption-event",
        "charter-service-adoption-correlation",
    );
    let changed_error = service
        .approve(changed_principal, None)
        .await
        .expect_err("changed principal conflicts with immutable receipt");
    assert!(matches!(
        changed_error,
        services::ServiceError::Db(db::DbError::IdempotencyConflict)
    ));

    let current_charter_version: i64 =
        sqlx::query_scalar("SELECT version FROM project_charter WHERE id = ?")
            .bind(CHARTER_ID)
            .fetch_one(db.pool())
            .await
            .expect("current Charter version");
    let amendment_content = charter_content("The amended scope remains auditable.");
    let amendment_render = render_and_digest_charter(&amendment_content);
    let amendment_draft = service
        .save_revision(
            ProjectCharterRevisionCommand {
                project_id: PROJECT_ID.to_owned(),
                charter_id: CHARTER_ID.to_owned(),
                base_revision_id: Some(REVISION_ONE_ID.to_owned()),
                expected_digest: Some(initial_render.content_digest.clone()),
                project_mode: "compact".to_owned(),
                maturity: "mvp".to_owned(),
                content: amendment_content,
                rendered_view: Some(amendment_render.rendered_view.clone()),
                render_version: Some(amendment_render.render_version.clone()),
                provenance: RevisionProvenance {
                    author: PrincipalRef {
                        kind: PrincipalKind::Agent,
                        id: IDENTITY_ID.to_owned(),
                        display_name: None,
                    },
                    profile_revision: Some(PROFILE_ID.to_owned()),
                    operating_skill_revision: Some(skill_revision_id.clone()),
                    source_refs: Vec::new(),
                    change_summary: "material scope change".to_owned(),
                    material_diff: None,
                },
                expected_charter_version: current_charter_version,
                idempotency_key: "charter-service-amendment-draft".to_owned(),
                authorization: authorization(
                    "agent",
                    IDENTITY_ID,
                    "project_charter.revision.save",
                    "charter-service-amendment-draft-event",
                    "charter-service-amendment-draft-correlation",
                ),
            },
            None,
        )
        .await
        .expect("service amendment draft");
    assert_eq!(amendment_draft.revision.revision, 2);

    let action_id = "charter-service-amendment-action";
    let amendment_key = "charter-service-amendment-command";
    AgentActionRepo::create_action(
        &*db,
        CreateAgentAction {
            id: action_id.to_owned(),
            actor_identity_id: IDENTITY_ID.to_owned(),
            scope_type: "project".to_owned(),
            scope_id: PROJECT_ID.to_owned(),
            operation: PROJECT_CHARTER_APPROVAL_COMMAND.to_owned(),
            payload_json: "{}".to_owned(),
            payload_hash: "charter-service-amendment-action-payload".to_owned(),
            dedupe_key: "charter-service-amendment-action-dedupe".to_owned(),
            correlation_id: amendment_key.to_owned(),
            causation_id: None,
            causation_depth: 0,
            requested_permission: "project.charter.approval".to_owned(),
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
    .expect("amendment AgentAction creates");

    let amendment = ProjectCharterApprovalCommand {
        project_id: PROJECT_ID.to_owned(),
        charter_id: CHARTER_ID.to_owned(),
        revision_id: amendment_draft.revision.id.clone(),
        content_digest: amendment_draft.revision.content_digest.clone(),
        rendered_digest: amendment_draft.revision.rendered_digest.clone(),
        expected_charter_version: amendment_draft.charter_version,
        expected_project_version: adopted.project_version,
        approved_project_name: "Adopted Charter Project".to_owned(),
        approved_project_slug: Some("adopted-charter-project".to_owned()),
        project_mode: "compact".to_owned(),
        selected_project_agent_identity_id: IDENTITY_ID.to_owned(),
        selected_project_agent_profile_revision_id: PROFILE_ID.to_owned(),
        selected_project_agent_operating_skill_revision: skill_revision_id.clone(),
        selected_project_agent_policy_digest: policy_digest(PROJECT_AGENT_TOOL_POLICY),
        idempotency_key: amendment_key.to_owned(),
        authorization: authorization(
            "agent",
            IDENTITY_ID,
            "project_charter.approval",
            "charter-service-amendment-event",
            amendment_key,
        ),
    };
    let amendment_action = AgentActionProvenance::new(
        action_id.to_owned(),
        1,
        1,
        amendment_key.to_owned(),
        "agent".to_owned(),
        IDENTITY_ID.to_owned(),
    );
    let amended = service
        .approve(amendment.clone(), Some(amendment_action.clone()))
        .await
        .expect("native action-backed amendment");
    assert_eq!(amended.project_charter_revision_id, amendment.revision_id);
    assert!(amended.amendment_id.is_some());

    let execution_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_action_execution
         WHERE action_id = ? AND idempotency_key = ?",
    )
    .bind(action_id)
    .bind(amendment_key)
    .fetch_one(db.pool())
    .await
    .expect("amendment action execution");
    assert_eq!(execution_count, 1);

    sqlx::query(
        r#"UPDATE project_agent_binding
         SET autonomy_policy_json = '{"changed_again":true}'
         WHERE project_id = ? AND state = 'active'"#,
    )
    .bind(PROJECT_ID)
    .execute(db.pool())
    .await
    .expect("amendment binding changes");
    let amendment_replay = service
        .approve(amendment, Some(amendment_action))
        .await
        .expect("exact action-backed amendment replay");
    assert_eq!(amendment_replay, amended);

    let receipt_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM command_receipt
         WHERE scope_type = 'project' AND scope_id = ?
           AND operation = 'project.charter.approval'",
    )
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("Charter command receipt count");
    assert_eq!(receipt_count, 2);
    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event
         WHERE event_type = 'project.charter.approved' AND scope_id = ?",
    )
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("Charter approval event count");
    assert_eq!(event_count, 2);
    let old_revision_lifecycle: String =
        sqlx::query_scalar("SELECT lifecycle FROM project_charter_revision WHERE id = ?")
            .bind(REVISION_ONE_ID)
            .fetch_one(db.pool())
            .await
            .expect("superseded prior Charter revision");
    assert_eq!(old_revision_lifecycle, "superseded");
}

#[tokio::test]
async fn semantic_first_revision_retry_finalizes_receipt_without_duplicate_revision() {
    let (db, skill_revision_id) = fixture().await;
    sqlx::query(
        "UPDATE project_agent_binding
         SET identity_id = ?, profile_id = ?, state = 'active',
             permission_ceiling_json = ?
         WHERE project_id = ? AND state = 'agent_setup_required'",
    )
    .bind(IDENTITY_ID)
    .bind(PROFILE_ID)
    .bind(PROJECT_AGENT_TOOL_POLICY)
    .bind(PROJECT_ID)
    .execute(db.pool())
    .await
    .expect("fixture Project Agent binding becomes active");

    let content = charter_content("A replay returns the same identifiers.");
    let rendered = render_and_digest_charter(&content);
    let command = ProjectCharterRevisionCommand {
        project_id: PROJECT_ID.to_owned(),
        charter_id: CHARTER_ID.to_owned(),
        base_revision_id: None,
        expected_digest: None,
        project_mode: "compact".to_owned(),
        maturity: "mvp".to_owned(),
        content,
        rendered_view: Some(rendered.rendered_view.clone()),
        render_version: Some(rendered.render_version.clone()),
        provenance: RevisionProvenance {
            author: PrincipalRef {
                kind: PrincipalKind::Agent,
                id: IDENTITY_ID.to_owned(),
                display_name: None,
            },
            profile_revision: Some(PROFILE_ID.to_owned()),
            operating_skill_revision: Some(skill_revision_id),
            source_refs: Vec::new(),
            change_summary: "initial Charter".to_owned(),
            material_diff: None,
        },
        expected_charter_version: 1,
        idempotency_key: "charter-service-semantic-noop".to_owned(),
        authorization: authorization(
            "agent",
            IDENTITY_ID,
            "project_charter.revision.save",
            "charter-service-semantic-noop-event",
            "charter-service-semantic-noop-correlation",
        ),
    };
    let service = ProjectCharterCommandService::new(Arc::clone(&db));
    let first = service
        .save_revision(command.clone(), None)
        .await
        .expect("semantic retry finalizes its command");
    let replay = service
        .save_revision(command.clone(), None)
        .await
        .expect("same-key semantic retry replays the frozen receipt");
    assert_eq!(replay, first);

    let revision_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM project_charter_revision WHERE charter_id = ?")
            .bind(CHARTER_ID)
            .fetch_one(db.pool())
            .await
            .expect("Charter revision count");
    assert_eq!(revision_count, 1);
    let receipt_event_id: String = sqlx::query_scalar(
        "SELECT event_id FROM command_receipt
         WHERE scope_type = 'project' AND scope_id = ?
           AND operation = 'project.charter.adoption'
           AND idempotency_key = ?",
    )
    .bind(PROJECT_ID)
    .bind("charter-service-semantic-noop")
    .fetch_one(db.pool())
    .await
    .expect("semantic no-op receipt");
    let event_type: String = sqlx::query_scalar("SELECT event_type FROM domain_event WHERE id = ?")
        .bind(&receipt_event_id)
        .fetch_one(db.pool())
        .await
        .expect("semantic no-op event");
    assert_eq!(event_type, "project_charter.revision_noop");

    let mut changed = command;
    changed.content = charter_content("The semantic retry changed its content.");
    let changed_render = render_and_digest_charter(&changed.content);
    changed.rendered_view = Some(changed_render.rendered_view);
    changed.render_version = Some(changed_render.render_version);
    let changed_error = service
        .save_revision(changed, None)
        .await
        .expect_err("changed same-key input conflicts with the frozen receipt");
    assert!(matches!(
        changed_error,
        services::ServiceError::Db(db::DbError::IdempotencyConflict)
    ));
    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'project_charter.revision_noop'",
    )
    .fetch_one(db.pool())
    .await
    .expect("semantic no-op event count");
    assert_eq!(event_count, 1);
}
