use db::{
    create_sqlite_pool, now_rfc3339, run_migrations, AgentActionExecutionStatus,
    AgentActionPolicyResult, AgentActionRepo, AgentActionStatus, AgentProfileRepo, AgentRepo,
    AgentStatus, AppendProjectDocumentRevisionCommand, ApproveProjectDecisionCandidateCommand,
    ApproveProjectDocument, ApproveProjectDocumentCommand, CommandReceiptRepo, CreateAgent,
    CreateAgentAction, CreateAgentActionExecution, CreateAgentIdentity, CreateAgentProfile,
    CreateCommandReceipt, CreateProject, CreateProjectDecision, CreateProjectDecisionCandidate,
    CreateProjectDecisionCandidateCommand, CreateProjectDocument, CreateProjectDocumentCommand,
    CreateProjectDocumentRevision, CreateProjectDocumentShellCommand, DbError, DomainEventRepo,
    ProjectOrchestrationRepo, ProjectRepo, RejectProjectDecisionCandidateCommand, SqliteDb,
};
use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

async fn database() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    SqliteDb::new(pool)
}

async fn file_database(name: &str) -> (SqliteDb, PathBuf) {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("forge-{name}-{}-{nanos}.db", std::process::id()));
    let url = format!("sqlite://{}", path.display());
    let pool = create_sqlite_pool(&url).await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    (SqliteDb::new(pool), path)
}

async fn project(db: &SqliteDb, id: &str) {
    ProjectRepo::create(
        db,
        CreateProject {
            id: id.to_owned(),
            name: format!("Command Project {id}"),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: "2026-08-20T00:00:00.000Z".to_owned(),
            updated_at: "2026-08-20T00:00:00.000Z".to_owned(),
        },
    )
    .await
    .expect("project");
}

async fn active_project_agent(db: &SqliteDb, project_id: &str, identity_id: &str) {
    let now = "2026-08-20T00:00:00.000Z";
    let profile_id = format!("{identity_id}-profile");
    AgentRepo::create_identity_with_profile(
        db,
        CreateAgentIdentity {
            id: identity_id.to_owned(),
            name: "Command Project Agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "account".to_owned(),
            account_permission_ceiling: r#"{"permissions":["read_project","propose_project"]}"#
                .to_owned(),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
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
            tool_policy_json: r#"{"permissions":["read_project","propose_project"]}"#.to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("identity/profile");

    let binding_id: String = sqlx::query_scalar(
        "SELECT id FROM project_agent_binding
         WHERE project_id = ? AND state = 'agent_setup_required' LIMIT 1",
    )
    .bind(project_id)
    .fetch_one(db.pool())
    .await
    .expect("setup binding");
    sqlx::query(
        "UPDATE project_agent_binding
         SET identity_id = ?, profile_id = ?, state = 'active',
             permission_ceiling_json = ?, updated_at = ?
         WHERE id = ?",
    )
    .bind(identity_id)
    .bind(profile_id)
    .bind(r#"{"allowed":["read_project","propose_project"]}"#)
    .bind(now)
    .bind(binding_id)
    .execute(db.pool())
    .await
    .expect("activate binding");
}

fn receipt(
    operation: &str,
    key: &str,
    digest: &str,
    outcome: serde_json::Value,
) -> CreateCommandReceipt {
    CreateCommandReceipt {
        id: format!("receipt-{key}"),
        principal_type: "user".to_owned(),
        principal_id: "user-1".to_owned(),
        scope_type: "project".to_owned(),
        scope_id: "project-1".to_owned(),
        operation: operation.to_owned(),
        idempotency_key: key.to_owned(),
        input_digest: digest.to_owned(),
        policy_result: "allowed".to_owned(),
        correlation_id: format!("correlation-{key}"),
        causation_id: Some(format!("cause-{key}")),
        causation_depth: 1,
        event_id: "pending-event".to_owned(),
        agent_action_execution_id: None,
        outcome_json: outcome.to_string(),
        committed_at: now_rfc3339(),
    }
}

fn document_command(
    project_id: &str,
    document_id: &str,
    revision_id: &str,
    command_receipt: Option<CreateCommandReceipt>,
) -> CreateProjectDocumentCommand {
    CreateProjectDocumentCommand {
        document: CreateProjectDocument {
            id: document_id.to_owned(),
            project_id: project_id.to_owned(),
            kind: "research".to_owned(),
            title: "Command document".to_owned(),
            approval_policy: "user_or_project_agent".to_owned(),
            created_at: "2026-08-20T00:00:00.000Z".to_owned(),
            updated_at: "2026-08-20T00:00:00.000Z".to_owned(),
        },
        revision: CreateProjectDocumentRevision {
            id: revision_id.to_owned(),
            document_id: document_id.to_owned(),
            expected_document_version: 1,
            base_revision: 0,
            base_revision_id: None,
            lifecycle: "draft".to_owned(),
            schema_version: "document@1".to_owned(),
            render_version: "render@1".to_owned(),
            content_json: "{}".to_owned(),
            rendered_view: "# Command document".to_owned(),
            change_summary: "initial".to_owned(),
            author_type: "agent".to_owned(),
            author_id: Some("agent-1".to_owned()),
            source_refs_json: "[]".to_owned(),
            content_digest: "content-digest".to_owned(),
            rendered_digest: "rendered-digest".to_owned(),
            created_at: "2026-08-20T00:00:00.000Z".to_owned(),
        },
        command_receipt,
        action_execution: None,
    }
}

fn document_shell_command(
    project_id: &str,
    document_id: &str,
    receipt: CreateCommandReceipt,
) -> CreateProjectDocumentShellCommand {
    CreateProjectDocumentShellCommand {
        document: CreateProjectDocument {
            id: document_id.to_owned(),
            project_id: project_id.to_owned(),
            kind: "research".to_owned(),
            title: "Concurrent shell".to_owned(),
            approval_policy: "user_or_project_agent".to_owned(),
            created_at: "2026-08-20T00:00:00.000Z".to_owned(),
            updated_at: "2026-08-20T00:00:00.000Z".to_owned(),
        },
        expected_project_version: 1,
        command_receipt: Some(receipt),
        action_execution: None,
    }
}

async fn install_receipt_failpoint(db: &SqliteDb, trigger_name: &str, message: &str) {
    let sql = format!(
        "CREATE TEMP TRIGGER {trigger_name}
         BEFORE INSERT ON command_receipt
         BEGIN SELECT RAISE(ABORT, '{message}'); END;"
    );
    sqlx::query(&sql)
        .execute(db.pool())
        .await
        .expect("command receipt failpoint");
}

async fn remove_receipt_failpoint(db: &SqliteDb, trigger_name: &str) {
    sqlx::query(&format!("DROP TRIGGER {trigger_name}"))
        .execute(db.pool())
        .await
        .expect("remove command receipt failpoint");
}

#[tokio::test]
async fn direct_project_writer_reauthorization_rejects_revoked_binding_atomically() {
    let db = database().await;
    project(&db, "project-1").await;
    active_project_agent(&db, "project-1", "agent-direct").await;
    sqlx::query(
        "UPDATE project_agent_binding
         SET state = 'revoked', updated_at = ?
         WHERE project_id = ? AND identity_id = ?",
    )
    .bind("2026-08-20T00:00:01.000Z")
    .bind("project-1")
    .bind("agent-direct")
    .execute(db.pool())
    .await
    .expect("revoke binding before writer authorization");

    let mut command_receipt = receipt(
        "project.document",
        "direct-revoked-binding",
        "direct-revoked-binding-digest",
        serde_json::json!({
            "operation": "project.document",
            "project_id": "project-1",
            "document_id": "document-revoked",
            "revision_id": "revision-revoked",
        }),
    );
    command_receipt.principal_type = "agent".to_owned();
    command_receipt.principal_id = "agent-direct".to_owned();

    let result = ProjectOrchestrationRepo::create_project_document_command(
        &db,
        document_command(
            "project-1",
            "document-revoked",
            "revision-revoked",
            Some(command_receipt),
        ),
    )
    .await;
    assert!(
        result.is_err(),
        "revoked binding must fail in the writer transaction"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_document WHERE id = 'document-revoked'",
        )
        .fetch_one(db.pool())
        .await
        .expect("document count"),
        0,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_document_revision WHERE id = 'revision-revoked'",
        )
        .fetch_one(db.pool())
        .await
        .expect("revision count"),
        0,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE entity_id = 'revision-revoked'",
        )
        .fetch_one(db.pool())
        .await
        .expect("event count"),
        0,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE id = 'receipt-direct-revoked-binding'",
        )
        .fetch_one(db.pool())
        .await
        .expect("receipt count"),
        0,
    );
}

#[tokio::test]
async fn direct_project_writer_reauthorization_rejects_changed_policy_without_effects() {
    let db = database().await;
    project(&db, "project-1").await;
    active_project_agent(&db, "project-1", "agent-direct").await;
    AgentProfileRepo::create_profile(
        &db,
        CreateAgentProfile {
            id: "agent-direct-restricted-profile".to_owned(),
            identity_id: "agent-direct".to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "native".to_owned(),
            provider: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: r#"{"permissions":["read_project"]}"#.to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: "2026-08-20T00:00:01.000Z".to_owned(),
            updated_at: "2026-08-20T00:00:01.000Z".to_owned(),
        },
    )
    .await
    .expect("restricted profile");
    sqlx::query("UPDATE agent_identity SET selected_profile_id = ? WHERE id = ?")
        .bind("agent-direct-restricted-profile")
        .bind("agent-direct")
        .execute(db.pool())
        .await
        .expect("select changed profile");
    sqlx::query(
        "UPDATE project_agent_binding
         SET permission_ceiling_json = ?, policy_revision = ?, policy_digest = ?
         WHERE project_id = ? AND identity_id = ?",
    )
    .bind(r#"{"allowed":["read_project"]}"#)
    .bind("revoked-policy")
    .bind("revoked-policy-digest")
    .bind("project-1")
    .bind("agent-direct")
    .execute(db.pool())
    .await
    .expect("change binding policy");

    let mut command_receipt = receipt(
        "project.document",
        "direct-changed-policy",
        "direct-changed-policy-digest",
        serde_json::json!({
            "operation": "project.document",
            "project_id": "project-1",
            "document_id": "document-policy",
            "revision_id": "revision-policy",
        }),
    );
    command_receipt.principal_type = "agent".to_owned();
    command_receipt.principal_id = "agent-direct".to_owned();

    let result = ProjectOrchestrationRepo::create_project_document_command(
        &db,
        document_command(
            "project-1",
            "document-policy",
            "revision-policy",
            Some(command_receipt),
        ),
    )
    .await;
    assert!(
        result.is_err(),
        "changed policy must fail in the writer transaction"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_document WHERE id = 'document-policy'",
        )
        .fetch_one(db.pool())
        .await
        .expect("document count"),
        0,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_document_revision WHERE id = 'revision-policy'",
        )
        .fetch_one(db.pool())
        .await
        .expect("revision count"),
        0,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event WHERE entity_id = 'revision-policy'",
        )
        .fetch_one(db.pool())
        .await
        .expect("event count"),
        0,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE id = 'receipt-direct-changed-policy'",
        )
        .fetch_one(db.pool())
        .await
        .expect("receipt count"),
        0,
    );
}

#[tokio::test]
async fn document_command_is_atomic_and_replay_exact() {
    let db = database().await;
    project(&db, "project-1").await;
    let document_id = "document-1";
    let revision_id = "document-revision-1";
    let command_receipt = receipt(
        "project.document.create",
        "document-create-1",
        "digest-document-create-1",
        serde_json::json!({
            "operation": "project.document.create",
            "project_id": "project-1",
            "document_id": document_id,
            "revision_id": revision_id,
        }),
    );
    let input = document_command(
        "project-1",
        document_id,
        revision_id,
        Some(command_receipt.clone()),
    );
    let first = ProjectOrchestrationRepo::create_project_document_command(&db, input.clone())
        .await
        .expect("document command");
    let replay = ProjectOrchestrationRepo::create_project_document_command(&db, input)
        .await
        .expect("document replay");
    assert_eq!(first, replay);

    let stored_receipt = CommandReceiptRepo::get_command_receipt(
        &db,
        "user",
        "user-1",
        "project",
        "project-1",
        "project.document.create",
        "document-create-1",
        "digest-document-create-1",
    )
    .await
    .expect("receipt lookup")
    .expect("receipt");
    let event = DomainEventRepo::get_event(&db, &stored_receipt.event_id)
        .await
        .expect("event lookup")
        .expect("event");
    assert_eq!(event.actor_type, "user");
    assert_eq!(event.actor_id.as_deref(), Some("user-1"));
    assert_eq!(event.correlation_id, stored_receipt.correlation_id);
    assert_eq!(event.causation_id, stored_receipt.causation_id);
    assert_eq!(event.causation_depth, stored_receipt.causation_depth);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_document_revision WHERE document_id = ?",
        )
        .bind(document_id)
        .fetch_one(db.pool())
        .await
        .expect("revision count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM command_receipt WHERE operation = ?",)
            .bind("project.document.create")
            .fetch_one(db.pool())
            .await
            .expect("receipt count"),
        1
    );

    let mut altered = command_receipt;
    altered.input_digest = "different-digest".to_owned();
    let conflict = ProjectOrchestrationRepo::create_project_document_command(
        &db,
        document_command("project-1", document_id, revision_id, Some(altered)),
    )
    .await;
    assert!(matches!(conflict, Err(DbError::IdempotencyConflict)));
}

#[tokio::test]
async fn document_shell_receipt_failpoint_rolls_back_then_replays_with_frozen_id() {
    let db = database().await;
    project(&db, "project-1").await;
    let command_receipt = receipt(
        "project.document.shell",
        "document-shell-failpoint",
        "digest-document-shell-failpoint",
        serde_json::json!({
            "operation": "project.document.shell",
            "project_id": "project-1",
            "document_id": "document-shell-failpoint",
        }),
    );
    let input = document_shell_command("project-1", "document-shell-failpoint", command_receipt);
    install_receipt_failpoint(
        &db,
        "document_shell_receipt_failpoint",
        "document shell receipt failpoint",
    )
    .await;

    let stopped =
        ProjectOrchestrationRepo::create_project_document_shell_command(&db, input.clone())
            .await
            .expect_err("receipt failpoint stops document shell");
    assert!(
        stopped
            .to_string()
            .contains("document shell receipt failpoint"),
        "unexpected document shell failpoint error: {stopped}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT version FROM project WHERE id = 'project-1'")
            .fetch_one(db.pool())
            .await
            .expect("Project version after shell rollback"),
        1,
        "Project CAS rolls back with the shell receipt"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_document
             WHERE id = 'document-shell-failpoint'",
        )
        .fetch_one(db.pool())
        .await
        .expect("shell count after rollback"),
        0,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'project.document.created'
               AND entity_id = 'document-shell-failpoint'",
        )
        .fetch_one(db.pool())
        .await
        .expect("shell event count after rollback"),
        0,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE operation = 'project.document.shell'
               AND idempotency_key = 'document-shell-failpoint'",
        )
        .fetch_one(db.pool())
        .await
        .expect("shell receipt count after rollback"),
        0,
    );

    remove_receipt_failpoint(&db, "document_shell_receipt_failpoint").await;
    let first = {
        let recreated_db = SqliteDb::new(db.pool().clone());
        ProjectOrchestrationRepo::create_project_document_shell_command(
            &recreated_db,
            input.clone(),
        )
        .await
        .expect("document shell retry after rollback")
    };
    assert_eq!(first.id, "document-shell-failpoint");

    let replay = {
        let recreated_db = SqliteDb::new(db.pool().clone());
        ProjectOrchestrationRepo::create_project_document_shell_command(&recreated_db, input)
            .await
            .expect("document shell replay after DB handle recreation")
    };
    assert_eq!(replay, first, "replay returns the frozen shell identity");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_document
             WHERE id = 'document-shell-failpoint'",
        )
        .fetch_one(db.pool())
        .await
        .expect("final shell count"),
        1,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'project.document.created'
               AND entity_id = 'document-shell-failpoint'",
        )
        .fetch_one(db.pool())
        .await
        .expect("final shell event count"),
        1,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE operation = 'project.document.shell'
               AND idempotency_key = 'document-shell-failpoint'",
        )
        .fetch_one(db.pool())
        .await
        .expect("final shell receipt count"),
        1,
    );
}

#[tokio::test]
async fn document_revision_receipt_failpoint_rolls_back_then_replays_without_duplicate_revision() {
    let db = database().await;
    project(&db, "project-1").await;
    ProjectOrchestrationRepo::create_project_document_command(
        &db,
        document_command(
            "project-1",
            "document-revision-failpoint",
            "revision-one",
            None,
        ),
    )
    .await
    .expect("initial document revision");

    let command_receipt = receipt(
        "project.document.revision.append",
        "document-revision-failpoint",
        "digest-document-revision-failpoint",
        serde_json::json!({
            "operation": "project.document.revision.append",
            "project_id": "project-1",
            "document_id": "document-revision-failpoint",
            "revision_id": "revision-two-failpoint",
        }),
    );
    let input = AppendProjectDocumentRevisionCommand {
        revision: CreateProjectDocumentRevision {
            id: "revision-two-failpoint".to_owned(),
            document_id: "document-revision-failpoint".to_owned(),
            expected_document_version: 2,
            base_revision: 1,
            base_revision_id: Some("revision-one".to_owned()),
            lifecycle: "proposed".to_owned(),
            schema_version: "document@1".to_owned(),
            render_version: "render@1".to_owned(),
            content_json: "{\"updated\":true}".to_owned(),
            rendered_view: "# Updated".to_owned(),
            change_summary: "updated".to_owned(),
            author_type: "agent".to_owned(),
            author_id: Some("agent-1".to_owned()),
            source_refs_json: "[]".to_owned(),
            content_digest: "content-digest-two-failpoint".to_owned(),
            rendered_digest: "rendered-digest-two-failpoint".to_owned(),
            created_at: "2026-08-20T00:04:00.000Z".to_owned(),
        },
        command_receipt: Some(command_receipt),
        action_execution: None,
    };
    install_receipt_failpoint(
        &db,
        "document_revision_receipt_failpoint",
        "document revision receipt failpoint",
    )
    .await;

    let stopped =
        ProjectOrchestrationRepo::append_project_document_revision_command(&db, input.clone())
            .await
            .expect_err("receipt failpoint stops document revision");
    assert!(
        stopped
            .to_string()
            .contains("document revision receipt failpoint"),
        "unexpected document revision failpoint error: {stopped}"
    );
    let document =
        ProjectOrchestrationRepo::get_project_document(&db, "document-revision-failpoint")
            .await
            .expect("document after revision rollback")
            .expect("document exists after revision rollback");
    assert_eq!(document.version, 2);
    assert_eq!(
        document.current_draft_revision_id.as_deref(),
        Some("revision-one")
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_document_revision
             WHERE document_id = 'document-revision-failpoint'",
        )
        .fetch_one(db.pool())
        .await
        .expect("revision count after rollback"),
        1,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'project.document.revision_created'
               AND entity_id = 'revision-two-failpoint'",
        )
        .fetch_one(db.pool())
        .await
        .expect("revision event count after rollback"),
        0,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE operation = 'project.document.revision.append'
               AND idempotency_key = 'document-revision-failpoint'",
        )
        .fetch_one(db.pool())
        .await
        .expect("revision receipt count after rollback"),
        0,
    );

    remove_receipt_failpoint(&db, "document_revision_receipt_failpoint").await;
    let first = {
        let recreated_db = SqliteDb::new(db.pool().clone());
        ProjectOrchestrationRepo::append_project_document_revision_command(
            &recreated_db,
            input.clone(),
        )
        .await
        .expect("document revision retry after rollback")
    };
    assert_eq!(first.id, "revision-two-failpoint");
    let replay = {
        let recreated_db = SqliteDb::new(db.pool().clone());
        ProjectOrchestrationRepo::append_project_document_revision_command(&recreated_db, input)
            .await
            .expect("document revision replay after DB handle recreation")
    };
    assert_eq!(replay, first, "replay returns the frozen revision identity");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_document_revision
             WHERE document_id = 'document-revision-failpoint'",
        )
        .fetch_one(db.pool())
        .await
        .expect("final revision count"),
        2,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'project.document.revision_created'
               AND entity_id = 'revision-two-failpoint'",
        )
        .fetch_one(db.pool())
        .await
        .expect("final revision event count"),
        1,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE operation = 'project.document.revision.append'
               AND idempotency_key = 'document-revision-failpoint'",
        )
        .fetch_one(db.pool())
        .await
        .expect("final revision receipt count"),
        1,
    );
}

#[tokio::test]
async fn document_approval_receipt_failpoint_rolls_back_then_replays_without_duplicate_approval() {
    let db = database().await;
    project(&db, "project-1").await;
    let document = ProjectOrchestrationRepo::create_project_document_command(
        &db,
        document_command(
            "project-1",
            "document-approval-failpoint",
            "revision-approval-failpoint",
            None,
        ),
    )
    .await
    .expect("document for approval");
    let approval_id = "document-approval-failpoint";
    let command_receipt = receipt(
        "project.document.approve",
        "document-approval-failpoint-key",
        "digest-document-approval-failpoint",
        serde_json::json!({
            "operation": "project.document.approve",
            "project_id": "project-1",
            "document_id": "document-approval-failpoint",
            "revision_id": "revision-approval-failpoint",
            "approval_id": approval_id,
        }),
    );
    let input = ApproveProjectDocumentCommand {
        approval: ApproveProjectDocument {
            id: approval_id.to_owned(),
            document_id: "document-approval-failpoint".to_owned(),
            revision_id: document.id.clone(),
            expected_document_version: 2,
            principal_type: "user".to_owned(),
            principal_id: "user-1".to_owned(),
            authorization_basis: "explicit user approval".to_owned(),
            authorization_action: "project.document.approve".to_owned(),
            explicit_event: "approval-failpoint-authorized".to_owned(),
            authorization_occurred_at: "2026-08-20T00:05:00.000Z".to_owned(),
            content_digest: "content-digest".to_owned(),
            rendered_digest: "rendered-digest".to_owned(),
            idempotency_key: "document-approval-domain-failpoint".to_owned(),
            created_at: "2026-08-20T00:05:00.000Z".to_owned(),
            updated_at: "2026-08-20T00:05:00.000Z".to_owned(),
        },
        command_receipt: Some(command_receipt),
        action_execution: None,
    };
    install_receipt_failpoint(
        &db,
        "document_approval_receipt_failpoint",
        "document approval receipt failpoint",
    )
    .await;

    let stopped = ProjectOrchestrationRepo::approve_project_document_command(&db, input.clone())
        .await
        .expect_err("receipt failpoint stops document approval");
    assert!(
        stopped
            .to_string()
            .contains("document approval receipt failpoint"),
        "unexpected document approval failpoint error: {stopped}"
    );
    let document_after_rollback =
        ProjectOrchestrationRepo::get_project_document(&db, "document-approval-failpoint")
            .await
            .expect("document after approval rollback")
            .expect("document exists after approval rollback");
    assert_eq!(document_after_rollback.version, 2);
    assert_eq!(
        document_after_rollback.current_draft_revision_id.as_deref(),
        Some("revision-approval-failpoint")
    );
    assert!(document_after_rollback
        .current_approved_revision_id
        .is_none());
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT lifecycle FROM project_document_revision
             WHERE id = 'revision-approval-failpoint'",
        )
        .fetch_one(db.pool())
        .await
        .expect("revision lifecycle after approval rollback"),
        "draft"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_document_approval
             WHERE id = 'document-approval-failpoint'",
        )
        .fetch_one(db.pool())
        .await
        .expect("approval count after rollback"),
        0,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'project.document.approved'
               AND entity_id = 'document-approval-failpoint'",
        )
        .fetch_one(db.pool())
        .await
        .expect("approval event count after rollback"),
        0,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE operation = 'project.document.approve'
               AND idempotency_key = 'document-approval-failpoint-key'",
        )
        .fetch_one(db.pool())
        .await
        .expect("approval receipt count after rollback"),
        0,
    );

    remove_receipt_failpoint(&db, "document_approval_receipt_failpoint").await;
    let first = {
        let recreated_db = SqliteDb::new(db.pool().clone());
        ProjectOrchestrationRepo::approve_project_document_command(&recreated_db, input.clone())
            .await
            .expect("document approval retry after rollback")
    };
    assert_eq!(first.id, approval_id);
    assert_eq!(first.lifecycle, "active");
    let replay = {
        let recreated_db = SqliteDb::new(db.pool().clone());
        ProjectOrchestrationRepo::approve_project_document_command(&recreated_db, input)
            .await
            .expect("document approval replay after DB handle recreation")
    };
    assert_eq!(replay, first, "replay returns the frozen approval identity");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_document_approval
             WHERE id = 'document-approval-failpoint'",
        )
        .fetch_one(db.pool())
        .await
        .expect("final approval count"),
        1,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'project.document.approved'
               AND entity_id = 'document-approval-failpoint'",
        )
        .fetch_one(db.pool())
        .await
        .expect("final approval event count"),
        1,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE operation = 'project.document.approve'
               AND idempotency_key = 'document-approval-failpoint-key'",
        )
        .fetch_one(db.pool())
        .await
        .expect("final approval receipt count"),
        1,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_document_shell_replay_returns_the_first_server_minted_id() {
    let (db, path) = file_database("document-shell-race").await;
    project(&db, "project-1").await;
    let first_receipt = receipt(
        "project.document.shell",
        "document-shell-race",
        "digest-document-shell-race",
        serde_json::json!({
            "operation": "project.document.shell",
            "project_id": "project-1",
            "document_id": "document-first",
        }),
    );
    let second_receipt = CreateCommandReceipt {
        id: "receipt-document-shell-race-second".to_owned(),
        outcome_json: serde_json::json!({
            "operation": "project.document.shell",
            "project_id": "project-1",
            "document_id": "document-second",
        })
        .to_string(),
        ..first_receipt.clone()
    };
    let first_input = document_shell_command("project-1", "document-first", first_receipt);
    let second_input = document_shell_command("project-1", "document-second", second_receipt);
    let (first, second) = tokio::join!(
        ProjectOrchestrationRepo::create_project_document_shell_command(&db, first_input),
        ProjectOrchestrationRepo::create_project_document_shell_command(&db, second_input),
    );
    let first = first.expect("first shell command");
    let second = second.expect("concurrent shell replay");
    assert_eq!(second, first);
    assert!(matches!(
        first.id.as_str(),
        "document-first" | "document-second"
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project_document")
            .fetch_one(db.pool())
            .await
            .expect("document count"),
        1
    );
    db.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_document_create_replay_returns_the_first_revision() {
    let (db, path) = file_database("document-create-race").await;
    project(&db, "project-1").await;
    let first_receipt = receipt(
        "project.document.create",
        "document-create-race",
        "digest-document-create-race",
        serde_json::json!({
            "operation": "project.document.create",
            "project_id": "project-1",
            "document_id": "document-first",
            "revision_id": "revision-first",
        }),
    );
    let second_receipt = CreateCommandReceipt {
        id: "receipt-document-create-race-second".to_owned(),
        outcome_json: serde_json::json!({
            "operation": "project.document.create",
            "project_id": "project-1",
            "document_id": "document-second",
            "revision_id": "revision-second",
        })
        .to_string(),
        ..first_receipt.clone()
    };
    let first_input = document_command(
        "project-1",
        "document-first",
        "revision-first",
        Some(first_receipt),
    );
    let second_input = document_command(
        "project-1",
        "document-second",
        "revision-second",
        Some(second_receipt),
    );
    let (first, second) = tokio::join!(
        ProjectOrchestrationRepo::create_project_document_command(&db, first_input),
        ProjectOrchestrationRepo::create_project_document_command(&db, second_input),
    );
    let first = first.expect("first document command");
    let second = second.expect("concurrent document replay");
    assert_eq!(second, first);
    assert!(matches!(
        first.id.as_str(),
        "revision-first" | "revision-second"
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project_document_revision")
            .fetch_one(db.pool())
            .await
            .expect("revision count"),
        1
    );
    db.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn document_command_links_action_execution_receipt_and_event() {
    let db = database().await;
    project(&db, "project-1").await;
    let now = now_rfc3339();
    AgentRepo::create(
        &db,
        CreateAgent {
            id: "user-1".to_owned(),
            name: "Command executor".to_owned(),
            description: None,
            executor_type: "native".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: db::AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "account".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("identity");
    let outcome = serde_json::json!({
        "operation": "project.document.create",
        "project_id": "project-1",
        "document_id": "document-action",
        "revision_id": "revision-action",
    });
    let mut command_receipt = receipt(
        "project.document.create",
        "document-action-1",
        "digest-document-action-1",
        outcome.clone(),
    );
    command_receipt.policy_result = "approval_required".to_owned();
    command_receipt.agent_action_execution_id = Some("execution-action".to_owned());
    let project_chat_id: String = sqlx::query_scalar(
        "SELECT id FROM agent_chat WHERE kind = 'project' AND project_id = 'project-1'",
    )
    .fetch_one(db.pool())
    .await
    .expect("canonical Project Chat");
    let action = AgentActionRepo::create_action(
        &db,
        CreateAgentAction {
            id: "action-document".to_owned(),
            actor_identity_id: "user-1".to_owned(),
            scope_type: "agent_chat".to_owned(),
            scope_id: project_chat_id,
            operation: "project.document.create".to_owned(),
            payload_json: "{}".to_owned(),
            payload_hash: "payload-hash".to_owned(),
            dedupe_key: "document-action-dedupe".to_owned(),
            correlation_id: command_receipt.correlation_id.clone(),
            causation_id: command_receipt.causation_id.clone(),
            causation_depth: command_receipt.causation_depth,
            requested_permission: "project.document.create".to_owned(),
            policy_result: AgentActionPolicyResult::ApprovalRequired,
            policy_reason: None,
            status: AgentActionStatus::Approved,
            target_type: Some("project_document".to_owned()),
            target_id: Some("document-action".to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("action");
    let execution = CreateAgentActionExecution {
        id: "execution-action".to_owned(),
        action_id: action.id,
        expected_action_version: action.version,
        attempt: 1,
        status: AgentActionExecutionStatus::Succeeded,
        result_json: Some(outcome.to_string()),
        error: None,
        executed_by_type: "user".to_owned(),
        executed_by_id: "user-1".to_owned(),
        idempotency_key: command_receipt.idempotency_key.clone(),
        action_status: AgentActionStatus::Executed,
        action_outcome_json: Some(outcome.to_string()),
        created_at: now.clone(),
        completed_at: Some(now.clone()),
        updated_at: now,
    };
    let mut input = document_command(
        "project-1",
        "document-action",
        "revision-action",
        Some(command_receipt.clone()),
    );
    input.action_execution = Some(execution);
    let revision = ProjectOrchestrationRepo::create_project_document_command(&db, input)
        .await
        .expect("action-backed document command");
    assert_eq!(revision.id, "revision-action");
    let stored = CommandReceiptRepo::get_command_receipt(
        &db,
        "user",
        "user-1",
        "project",
        "project-1",
        "project.document.create",
        "document-action-1",
        "digest-document-action-1",
    )
    .await
    .expect("receipt lookup")
    .expect("receipt");
    assert_eq!(
        stored.agent_action_execution_id.as_deref(),
        Some("execution-action")
    );
    let execution = AgentActionRepo::list_action_executions(&db, "action-document")
        .await
        .expect("execution list");
    assert_eq!(execution.len(), 1);
    assert_eq!(execution[0].id, "execution-action");
    assert_eq!(
        execution[0].result_json.as_deref(),
        Some(outcome.to_string().as_str())
    );
    let event = DomainEventRepo::get_event(&db, &stored.event_id)
        .await
        .expect("event lookup")
        .expect("event");
    assert_eq!(event.actor_id.as_deref(), Some("user-1"));
}

#[tokio::test]
async fn document_command_rolls_back_before_receipt_finalization() {
    let db = database().await;
    project(&db, "project-1").await;
    let mut command_receipt = receipt(
        "project.document.create",
        "document-rollback-1",
        "digest-document-rollback-1",
        serde_json::json!({
            "document_id": "document-rollback",
            "revision_id": "revision-rollback",
        }),
    );
    command_receipt.agent_action_execution_id = Some("execution-rollback".to_owned());
    let action_execution = CreateAgentActionExecution {
        id: "execution-rollback".to_owned(),
        action_id: "action-that-is-not-written".to_owned(),
        expected_action_version: 1,
        attempt: 1,
        status: AgentActionExecutionStatus::Succeeded,
        result_json: Some("{\"different\":true}".to_owned()),
        error: None,
        executed_by_type: "user".to_owned(),
        executed_by_id: "user-1".to_owned(),
        idempotency_key: "document-rollback-1".to_owned(),
        action_status: AgentActionStatus::Executed,
        action_outcome_json: Some("{\"different\":true}".to_owned()),
        created_at: now_rfc3339(),
        completed_at: Some(now_rfc3339()),
        updated_at: now_rfc3339(),
    };
    let mut input = document_command(
        "project-1",
        "document-rollback",
        "revision-rollback",
        Some(command_receipt),
    );
    input.action_execution = Some(action_execution);
    let result = ProjectOrchestrationRepo::create_project_document_command(&db, input).await;
    assert!(matches!(result, Err(DbError::IdempotencyConflict)));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_document WHERE id = 'document-rollback'",
        )
        .fetch_one(db.pool())
        .await
        .expect("document count"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event WHERE entity_id = 'revision-rollback'",
        )
        .fetch_one(db.pool())
        .await
        .expect("event count"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt WHERE id = 'receipt-document-rollback-1'",
        )
        .fetch_one(db.pool())
        .await
        .expect("receipt count"),
        0
    );
}

#[tokio::test]
async fn document_approval_command_replays_and_keeps_event_linkage() {
    let db = database().await;
    project(&db, "project-1").await;
    let document = ProjectOrchestrationRepo::create_project_document_command(
        &db,
        document_command("project-1", "document-approval", "revision-approval", None),
    )
    .await
    .expect("document");
    let now = "2026-08-20T00:01:00.000Z";
    let approval_id = "document-approval-record";
    let approval_receipt = receipt(
        "project.document.approve",
        "document-approval-1",
        "digest-document-approval-1",
        serde_json::json!({
            "document_id": "document-approval",
            "revision_id": "revision-approval",
            "approval_id": approval_id,
        }),
    );
    let input = ApproveProjectDocumentCommand {
        approval: ApproveProjectDocument {
            id: approval_id.to_owned(),
            document_id: "document-approval".to_owned(),
            revision_id: document.id.clone(),
            expected_document_version: 2,
            principal_type: "user".to_owned(),
            principal_id: "user-1".to_owned(),
            authorization_basis: "explicit user approval".to_owned(),
            authorization_action: "project.document.approve".to_owned(),
            explicit_event: "approval-authorized".to_owned(),
            authorization_occurred_at: now.to_owned(),
            content_digest: "content-digest".to_owned(),
            rendered_digest: "rendered-digest".to_owned(),
            idempotency_key: "document-approval-domain-key".to_owned(),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
        command_receipt: Some(approval_receipt.clone()),
        action_execution: None,
    };
    let first = ProjectOrchestrationRepo::approve_project_document_command(&db, input.clone())
        .await
        .expect("approval");
    assert_eq!(first.revision_id, "revision-approval");
    let replay = ProjectOrchestrationRepo::approve_project_document_command(&db, input)
        .await
        .expect("approval replay");
    assert_eq!(replay, first);
    let stored = CommandReceiptRepo::get_command_receipt(
        &db,
        "user",
        "user-1",
        "project",
        "project-1",
        "project.document.approve",
        "document-approval-1",
        "digest-document-approval-1",
    )
    .await
    .expect("approval receipt")
    .expect("stored approval receipt");
    let event = DomainEventRepo::get_event(&db, &stored.event_id)
        .await
        .expect("approval event")
        .expect("stored approval event");
    assert_eq!(event.event_type, "project.document.approved");
    assert_eq!(event.actor_id.as_deref(), Some("user-1"));
}

#[tokio::test]
async fn document_revision_append_command_replays_without_a_second_pointer() {
    let db = database().await;
    project(&db, "project-1").await;
    ProjectOrchestrationRepo::create_project_document_command(
        &db,
        document_command("project-1", "document-revisions", "revision-one", None),
    )
    .await
    .expect("first revision");
    let now = "2026-08-20T00:03:00.000Z";
    let command_receipt = receipt(
        "project.document.revision.append",
        "document-revision-append-1",
        "digest-document-revision-append-1",
        serde_json::json!({
            "document_id": "document-revisions",
            "revision_id": "revision-two",
        }),
    );
    let input = AppendProjectDocumentRevisionCommand {
        revision: CreateProjectDocumentRevision {
            id: "revision-two".to_owned(),
            document_id: "document-revisions".to_owned(),
            expected_document_version: 2,
            base_revision: 1,
            base_revision_id: Some("revision-one".to_owned()),
            lifecycle: "proposed".to_owned(),
            schema_version: "document@1".to_owned(),
            render_version: "render@1".to_owned(),
            content_json: "{\"updated\":true}".to_owned(),
            rendered_view: "# Updated".to_owned(),
            change_summary: "updated".to_owned(),
            author_type: "agent".to_owned(),
            author_id: Some("agent-1".to_owned()),
            source_refs_json: "[]".to_owned(),
            content_digest: "content-digest-two".to_owned(),
            rendered_digest: "rendered-digest-two".to_owned(),
            created_at: now.to_owned(),
        },
        command_receipt: Some(command_receipt),
        action_execution: None,
    };
    let first =
        ProjectOrchestrationRepo::append_project_document_revision_command(&db, input.clone())
            .await
            .expect("append revision");
    let replay = ProjectOrchestrationRepo::append_project_document_revision_command(&db, input)
        .await
        .expect("append replay");
    assert_eq!(first, replay);
    let document = ProjectOrchestrationRepo::get_project_document(&db, "document-revisions")
        .await
        .expect("document lookup")
        .expect("document");
    assert_eq!(document.version, 3);
    assert_eq!(
        document.current_draft_revision_id.as_deref(),
        Some("revision-two")
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_document_revision WHERE document_id = ?",
        )
        .bind("document-revisions")
        .fetch_one(db.pool())
        .await
        .expect("revision count"),
        2
    );
}

fn candidate_command(
    project_id: &str,
    candidate_id: &str,
    receipt: Option<CreateCommandReceipt>,
) -> CreateProjectDecisionCandidateCommand {
    CreateProjectDecisionCandidateCommand {
        candidate: CreateProjectDecisionCandidate {
            id: candidate_id.to_owned(),
            project_id: project_id.to_owned(),
            lifecycle: "proposed".to_owned(),
            question: "Which implementation choice?".to_owned(),
            context_json: serde_json::json!({"decision_class":"project_implementation"})
                .to_string(),
            options_json: "[\"a\",\"b\"]".to_owned(),
            selected_outcome: Some("a".to_owned()),
            rationale: Some("a is supported".to_owned()),
            principal_type: Some("user".to_owned()),
            principal_id: Some("user-1".to_owned()),
            source_refs_json: "[]".to_owned(),
            expected_project_version: 1,
            created_at: "2026-08-20T00:00:00.000Z".to_owned(),
            updated_at: "2026-08-20T00:00:00.000Z".to_owned(),
        },
        command_receipt: receipt,
        action_execution: None,
    }
}

#[tokio::test]
async fn decision_candidate_effective_and_rejection_commands_are_atomic() {
    let db = database().await;
    project(&db, "project-1").await;
    let candidate_receipt = receipt(
        "project.decision.candidate.create",
        "candidate-1",
        "digest-candidate-1",
        serde_json::json!({"candidate_id":"candidate-1"}),
    );
    let candidate = ProjectOrchestrationRepo::create_project_decision_candidate_command(
        &db,
        candidate_command("project-1", "candidate-1", Some(candidate_receipt.clone())),
    )
    .await
    .expect("candidate");
    let candidate_replay = ProjectOrchestrationRepo::create_project_decision_candidate_command(
        &db,
        candidate_command("project-1", "candidate-1", Some(candidate_receipt)),
    )
    .await
    .expect("candidate replay");
    assert_eq!(candidate, candidate_replay);

    let now = "2026-08-20T00:02:00.000Z";
    let decision_receipt = receipt(
        "project.decision.approve",
        "decision-1",
        "digest-decision-1",
        serde_json::json!({"candidate_id":"candidate-1","decision_id":"decision-1"}),
    );
    let decision = CreateProjectDecision {
        id: "decision-1".to_owned(),
        project_id: "project-1".to_owned(),
        expected_project_version: 2,
        state: "active".to_owned(),
        decision_class: "project_implementation".to_owned(),
        question: candidate.question.clone(),
        context_json: candidate.context_json.clone(),
        options_json: candidate.options_json.clone(),
        selected_outcome: "a".to_owned(),
        rationale: "a is supported".to_owned(),
        principal_type: "user".to_owned(),
        principal_id: "user-1".to_owned(),
        authority_basis: "explicit user approval".to_owned(),
        authorization_action: "project.decision.approve".to_owned(),
        explicit_event: "decision-approved".to_owned(),
        authorization_occurred_at: now.to_owned(),
        charter_revision_id: None,
        baseline_revision_id: None,
        source_refs_json: candidate.source_refs_json.clone(),
        affected_records_json: "{}".to_owned(),
        supersedes_decision_id: None,
        created_at: now.to_owned(),
    };
    let effective = ProjectOrchestrationRepo::approve_project_decision_candidate_command(
        &db,
        ApproveProjectDecisionCandidateCommand {
            candidate_id: "candidate-1".to_owned(),
            expected_candidate_version: 1,
            decision: decision.clone(),
            command_receipt: Some(decision_receipt.clone()),
            action_execution: None,
        },
    )
    .await
    .expect("effective decision");
    let effective_replay = ProjectOrchestrationRepo::approve_project_decision_candidate_command(
        &db,
        ApproveProjectDecisionCandidateCommand {
            candidate_id: "candidate-1".to_owned(),
            expected_candidate_version: 1,
            decision,
            command_receipt: Some(decision_receipt),
            action_execution: None,
        },
    )
    .await
    .expect("effective decision replay");
    assert_eq!(effective, effective_replay);

    project(&db, "project-reject").await;
    let mut reject_candidate_input = candidate_command("project-reject", "candidate-reject", None);
    reject_candidate_input.candidate.expected_project_version = 1;
    let reject_candidate = ProjectOrchestrationRepo::create_project_decision_candidate_command(
        &db,
        reject_candidate_input,
    )
    .await
    .expect("reject candidate");
    let mut reject_receipt = receipt(
        "project.decision.reject",
        "reject-1",
        "digest-reject-1",
        serde_json::json!({"candidate_id":"candidate-reject"}),
    );
    reject_receipt.scope_id = "project-reject".to_owned();
    let reject_input = RejectProjectDecisionCandidateCommand {
        candidate_id: reject_candidate.id.clone(),
        project_id: "project-reject".to_owned(),
        expected_project_version: 2,
        expected_candidate_version: 1,
        reason: "needs more evidence".to_owned(),
        principal_type: "user".to_owned(),
        principal_id: "user-1".to_owned(),
        authorization_basis: "explicit user rejection".to_owned(),
        authorization_action: "project.decision.reject".to_owned(),
        explicit_event: "decision-rejected".to_owned(),
        authorization_occurred_at: now.to_owned(),
        command_receipt: Some(reject_receipt.clone()),
        action_execution: None,
        updated_at: now.to_owned(),
    };
    let rejected = ProjectOrchestrationRepo::reject_project_decision_candidate_command(
        &db,
        reject_input.clone(),
    )
    .await
    .expect("reject");
    assert_eq!(rejected.lifecycle, "rejected");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&rejected.context_json)
            .expect("rejection context")["rejection_reason"],
        "needs more evidence"
    );
    let rejected_replay =
        ProjectOrchestrationRepo::reject_project_decision_candidate_command(&db, reject_input)
            .await
            .expect("reject replay");
    assert_eq!(rejected, rejected_replay);
}
