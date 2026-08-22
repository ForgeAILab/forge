use db::{
    now_rfc3339, run_migrations, AgentActionPolicyResult, AgentActionRepo, AgentActionStatus,
    AgentRepo, AgentStatus, CommentAuthorType, CreateAgentAction, CreateAgentActionExecution,
    CreateAgentIdentity, CreateAgentProfile, CreateCommandReceipt, CreateProjectMediaAttachment,
    CreateProjectMediaAttachmentMutation, CreateTaskMedia, DbError, SharedMediaRepo, SqliteDb,
    TaskMediaRepo,
};
use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

const PROJECT_ID: &str = "project-evidence-command";
const TASK_ID: &str = "task-evidence-command";
const MILESTONE_ID: &str = "milestone-evidence-command";
const ASSET_ID: &str = "asset-evidence-command";
const CHECKSUM: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

async fn fixture() -> SqliteDb {
    fixture_with_url("sqlite::memory:").await
}

async fn fixture_with_url(url: &str) -> SqliteDb {
    let pool = db::create_sqlite_pool(url).await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    let db = SqliteDb::new(pool);
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES ('evidence-user', 'evidence@example.test', 'test', 'Evidence Tester', ?, ?)",
    )
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("user");
    sqlx::query(
        "INSERT INTO project (id, name, owner_id, settings, created_at, updated_at)
         VALUES (?, 'Evidence Command Project', 'evidence-user', '{}', ?, ?)",
    )
    .bind(PROJECT_ID)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("project");
    sqlx::query(
        "INSERT INTO repo
         (id, project_id, name, remote_url, local_path, work_mode, default_branch,
          created_at, updated_at)
         VALUES ('repo-evidence-command', ?, 'Evidence Repo', '/tmp/evidence-repo',
                 '/tmp/evidence-repo', 'direct_merge', 'main', ?, ?)",
    )
    .bind(PROJECT_ID)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("repo");
    sqlx::query(
        "INSERT INTO task
         (id, project_id, repo_id, title, task_type, status, created_at, updated_at)
         VALUES (?, ?, 'repo-evidence-command', 'Evidence Task', 'task', 'todo', ?, ?)",
    )
    .bind(TASK_ID)
    .bind(PROJECT_ID)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("task");
    sqlx::query(
        "INSERT INTO project_milestone
         (id, project_id, milestone_sequence, milestone_key, lifecycle, created_at, updated_at)
         VALUES (?, ?, 1, 'M001', 'active', ?, ?)",
    )
    .bind(MILESTONE_ID)
    .bind(PROJECT_ID)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("milestone");

    let media = TaskMediaRepo::create_media(
        &db,
        CreateTaskMedia {
            id: ASSET_ID.to_owned(),
            task_id: TASK_ID.to_owned(),
            display_filename: "proof.png".to_owned(),
            content_type: "image/png".to_owned(),
            byte_size: 4,
            storage_key: "task-evidence-command/asset-evidence-command__proof.png".to_owned(),
            author_type: CommentAuthorType::User,
            author_id: Some("evidence-user".to_owned()),
            author_name: "Evidence Tester".to_owned(),
            created_at: now.clone(),
        },
    )
    .await
    .expect("task media");
    SharedMediaRepo::set_media_asset_checksum(&db, &media.id, media.byte_size, CHECKSUM, &now)
        .await
        .expect("asset checksum");
    db
}

async fn file_fixture() -> (SqliteDb, PathBuf) {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "forge-evidence-race-{}-{nanos}.db",
        std::process::id()
    ));
    let url = format!("sqlite://{}", path.display());
    (fixture_with_url(&url).await, path)
}

fn attachment(
    id: &str,
    author_type: &str,
    author_id: &str,
    created_at: &str,
) -> CreateProjectMediaAttachment {
    CreateProjectMediaAttachment {
        id: id.to_owned(),
        project_id: PROJECT_ID.to_owned(),
        asset_id: ASSET_ID.to_owned(),
        attachment_kind: "evidence".to_owned(),
        task_media_id: None,
        task_id: Some(TASK_ID.to_owned()),
        milestone_id: Some(MILESTONE_ID.to_owned()),
        milestone_check_id: None,
        source_task_id: Some(TASK_ID.to_owned()),
        source_execution_id: None,
        source_validation_id: None,
        acceptance_check_ids_json: "[]".to_owned(),
        caption: Some("command evidence".to_owned()),
        evidence_kind: Some("screenshot".to_owned()),
        checksum: Some(CHECKSUM.to_owned()),
        availability: "available".to_owned(),
        project_url: Some(format!("/api/v1/projects/{PROJECT_ID}/media/{ASSET_ID}")),
        author_type: author_type.to_owned(),
        author_id: Some(author_id.to_owned()),
        authorization_json: "{}".to_owned(),
        created_at: created_at.to_owned(),
    }
}

struct ReceiptSpec<'a> {
    id: &'a str,
    principal_type: &'a str,
    principal_id: &'a str,
    key: &'a str,
    digest: &'a str,
    outcome_json: &'a str,
    correlation_id: &'a str,
    execution_id: Option<&'a str>,
}

fn receipt(spec: ReceiptSpec<'_>) -> CreateCommandReceipt {
    CreateCommandReceipt {
        id: spec.id.to_owned(),
        principal_type: spec.principal_type.to_owned(),
        principal_id: spec.principal_id.to_owned(),
        scope_type: "project".to_owned(),
        scope_id: PROJECT_ID.to_owned(),
        operation: "project.evidence".to_owned(),
        idempotency_key: spec.key.to_owned(),
        input_digest: spec.digest.to_owned(),
        policy_result: "allowed".to_owned(),
        correlation_id: spec.correlation_id.to_owned(),
        causation_id: None,
        causation_depth: 0,
        event_id: String::new(),
        agent_action_execution_id: spec.execution_id.map(str::to_owned),
        outcome_json: spec.outcome_json.to_owned(),
        committed_at: now_rfc3339(),
    }
}

fn mutation(
    attachment: CreateProjectMediaAttachment,
    key: &str,
    digest: &str,
    command_receipt: Option<CreateCommandReceipt>,
    action_execution: Option<CreateAgentActionExecution>,
) -> CreateProjectMediaAttachmentMutation {
    CreateProjectMediaAttachmentMutation {
        attachment,
        expected_milestone_version: 1,
        idempotency_key: key.to_owned(),
        mutation_fingerprint: digest.to_owned(),
        authorization_event_id: format!("authorization-{key}"),
        command_receipt,
        action_execution,
    }
}

#[tokio::test]
async fn evidence_command_rolls_back_attachment_reconciliation_event_and_receipt() {
    let db = fixture().await;
    let now = now_rfc3339();
    let command_receipt = receipt(ReceiptSpec {
        id: "receipt-evidence-rollback",
        principal_type: "user",
        principal_id: "evidence-user",
        key: "evidence-rollback-key",
        digest: "command-rollback-digest",
        outcome_json: "not-json",
        correlation_id: "evidence-rollback-correlation",
        execution_id: None,
    });
    let result = SharedMediaRepo::create_project_media_attachment_mutation(
        &db,
        mutation(
            attachment("evidence-rollback", "user", "evidence-user", &now),
            "evidence-rollback-key",
            "evidence-rollback-domain-fingerprint",
            Some(command_receipt),
            None,
        ),
    )
    .await;
    assert!(
        matches!(result, Err(DbError::Check(_))),
        "unexpected result: {result:?}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_media_attachment WHERE id = 'evidence-rollback'",
        )
        .fetch_one(db.pool())
        .await
        .expect("attachment count"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event WHERE event_type = 'project.evidence.attached'",
        )
        .fetch_one(db.pool())
        .await
        .expect("event count"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt WHERE id = 'receipt-evidence-rollback'",
        )
        .fetch_one(db.pool())
        .await
        .expect("receipt count"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT gc_state FROM media_asset WHERE id = ?")
            .bind(ASSET_ID)
            .fetch_one(db.pool())
            .await
            .expect("asset state"),
        "referenced"
    );
}

#[tokio::test]
async fn evidence_command_replays_original_attachment_before_current_checks() {
    let db = fixture().await;
    let now = now_rfc3339();
    let outcome = r#"{"operation":"project.evidence","attachment_id":"evidence-replay"}"#;
    let command_receipt = receipt(ReceiptSpec {
        id: "receipt-evidence-replay",
        principal_type: "user",
        principal_id: "evidence-user",
        key: "evidence-replay-key",
        digest: "command-replay-digest",
        outcome_json: outcome,
        correlation_id: "evidence-replay-correlation",
        execution_id: None,
    });
    let first_input = mutation(
        attachment("evidence-replay", "user", "evidence-user", &now),
        "evidence-replay-key",
        "evidence-replay-domain-fingerprint",
        Some(command_receipt.clone()),
        None,
    );
    let first = SharedMediaRepo::create_project_media_attachment_mutation(&db, first_input.clone())
        .await
        .expect("first evidence command");

    // Change the mutable version after commit.  Receipt resolution must run
    // first and return the frozen attachment instead of re-validating this
    // now-stale expected version.
    sqlx::query("UPDATE project_milestone SET version = 2 WHERE id = ?")
        .bind(MILESTONE_ID)
        .execute(db.pool())
        .await
        .expect("advance milestone");
    let replay = SharedMediaRepo::create_project_media_attachment_mutation(&db, first_input)
        .await
        .expect("evidence replay");
    assert_eq!(replay, first);
    let stored_outcome: String = sqlx::query_scalar(
        "SELECT outcome_json FROM command_receipt WHERE id = 'receipt-evidence-replay'",
    )
    .fetch_one(db.pool())
    .await
    .expect("stored outcome");
    assert_eq!(stored_outcome, outcome);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_media_attachment
             WHERE project_id = ? AND attachment_kind = 'evidence'",
        )
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("attachment count"),
        1
    );

    let mut changed_digest = mutation(
        attachment("evidence-replay-changed", "user", "evidence-user", &now),
        "evidence-replay-key",
        "evidence-replay-domain-fingerprint-changed",
        Some(command_receipt.clone()),
        None,
    );
    changed_digest
        .command_receipt
        .as_mut()
        .unwrap()
        .input_digest = "command-replay-digest-changed".to_owned();
    assert!(matches!(
        SharedMediaRepo::create_project_media_attachment_mutation(&db, changed_digest).await,
        Err(DbError::IdempotencyConflict)
    ));

    let mut changed_principal = mutation(
        attachment(
            "evidence-replay-principal",
            "agent",
            "different-principal",
            &now,
        ),
        "evidence-replay-key",
        "command-replay-digest",
        Some(command_receipt),
        None,
    );
    changed_principal
        .command_receipt
        .as_mut()
        .unwrap()
        .principal_id = "different-principal".to_owned();
    assert!(matches!(
        SharedMediaRepo::create_project_media_attachment_mutation(&db, changed_principal).await,
        Err(DbError::IdempotencyConflict)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_evidence_replay_returns_the_first_attachment_id() {
    let (db, path) = file_fixture().await;
    let now = now_rfc3339();
    let first_receipt = receipt(ReceiptSpec {
        id: "receipt-evidence-race-first",
        principal_type: "user",
        principal_id: "evidence-user",
        key: "evidence-race-key",
        digest: "evidence-race-digest",
        outcome_json: r#"{"operation":"project.evidence","attachment_id":"evidence-race-first"}"#,
        correlation_id: "evidence-race-correlation",
        execution_id: None,
    });
    let second_receipt = CreateCommandReceipt {
        id: "receipt-evidence-race-second".to_owned(),
        outcome_json: r#"{"operation":"project.evidence","attachment_id":"evidence-race-second"}"#
            .to_owned(),
        ..first_receipt.clone()
    };
    let first = mutation(
        attachment("evidence-race-first", "user", "evidence-user", &now),
        "evidence-race-key",
        "evidence-race-domain-fingerprint",
        Some(first_receipt),
        None,
    );
    let second = mutation(
        attachment("evidence-race-second", "user", "evidence-user", &now),
        "evidence-race-key",
        "evidence-race-domain-fingerprint",
        Some(second_receipt),
        None,
    );
    let (first, second) = tokio::join!(
        SharedMediaRepo::create_project_media_attachment_mutation(&db, first),
        SharedMediaRepo::create_project_media_attachment_mutation(&db, second),
    );
    let first = first.expect("first evidence command");
    let second = second.expect("concurrent evidence replay");
    assert_eq!(second, first);
    assert!(matches!(
        first.id.as_str(),
        "evidence-race-first" | "evidence-race-second"
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_media_attachment
             WHERE project_id = ? AND attachment_kind = 'evidence'",
        )
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("evidence count"),
        1
    );
    db.pool().close().await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn evidence_command_links_event_receipt_and_action_execution_atomically() {
    let db = fixture().await;
    let now = now_rfc3339();
    let identity_id = "evidence-agent";
    AgentRepo::create_identity_with_profile(
        &db,
        CreateAgentIdentity {
            id: identity_id.to_owned(),
            name: "Evidence Agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some("evidence-user".to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: "evidence-agent-profile".to_owned(),
            identity_id: identity_id.to_owned(),
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
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("agent identity");
    let action = AgentActionRepo::create_action(
        &db,
        CreateAgentAction {
            id: "evidence-agent-action".to_owned(),
            actor_identity_id: identity_id.to_owned(),
            scope_type: "project".to_owned(),
            scope_id: PROJECT_ID.to_owned(),
            operation: "project.evidence".to_owned(),
            payload_json: "{}".to_owned(),
            payload_hash: "command-action-digest".to_owned(),
            dedupe_key: "evidence-action-dedupe".to_owned(),
            correlation_id: "evidence-action-correlation".to_owned(),
            causation_id: None,
            causation_depth: 0,
            requested_permission: "project.evidence.attach".to_owned(),
            policy_result: AgentActionPolicyResult::Allowed,
            policy_reason: None,
            status: AgentActionStatus::Approved,
            target_type: Some("project_media_attachment".to_owned()),
            target_id: Some("evidence-action".to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("agent action");
    let outcome = r#"{"operation":"project.evidence","attachment_id":"evidence-action"}"#;
    let execution_id = "evidence-agent-execution";
    let execution = CreateAgentActionExecution {
        id: execution_id.to_owned(),
        action_id: action.id.clone(),
        expected_action_version: action.version,
        attempt: 1,
        status: db::AgentActionExecutionStatus::Succeeded,
        result_json: Some(outcome.to_owned()),
        error: None,
        executed_by_type: "agent".to_owned(),
        executed_by_id: identity_id.to_owned(),
        idempotency_key: "evidence-action-key".to_owned(),
        action_status: AgentActionStatus::Executed,
        action_outcome_json: Some(outcome.to_owned()),
        created_at: now.clone(),
        completed_at: Some(now.clone()),
        updated_at: now.clone(),
    };
    let execution_for_replay = execution.clone();
    let command_receipt = receipt(ReceiptSpec {
        id: "receipt-evidence-action",
        principal_type: "agent",
        principal_id: identity_id,
        key: "evidence-action-key",
        digest: "command-action-digest",
        outcome_json: outcome,
        correlation_id: "evidence-action-correlation",
        execution_id: Some(execution_id),
    });
    let receipt_for_replay = command_receipt.clone();
    let committed = SharedMediaRepo::create_project_media_attachment_mutation(
        &db,
        mutation(
            attachment("evidence-action", "agent", identity_id, &now),
            "evidence-action-key",
            "evidence-action-domain-fingerprint",
            Some(command_receipt),
            Some(execution),
        ),
    )
    .await
    .expect("action-backed evidence command");
    let (event_actor_type, event_actor_id, event_correlation): (String, String, String) =
        sqlx::query_as(
            "SELECT actor_type, actor_id, correlation_id
             FROM domain_event WHERE entity_id = ?",
        )
        .bind(&committed.id)
        .fetch_one(db.pool())
        .await
        .expect("evidence event");
    assert_eq!(event_actor_type, "agent");
    assert_eq!(event_actor_id, identity_id);
    assert_eq!(event_correlation, "evidence-action-correlation");
    let (receipt_outcome, receipt_event_id, receipt_execution_id): (String, String, String) =
        sqlx::query_as(
            "SELECT outcome_json, event_id, agent_action_execution_id
             FROM command_receipt WHERE id = 'receipt-evidence-action'",
        )
        .fetch_one(db.pool())
        .await
        .expect("evidence receipt");
    assert_eq!(receipt_outcome, outcome);
    assert_eq!(receipt_execution_id, execution_id);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM domain_event WHERE id = ?")
            .bind(receipt_event_id)
            .fetch_one(db.pool())
            .await
            .expect("receipt event linkage"),
        1
    );
    let action_outcome: Option<String> =
        sqlx::query_scalar("SELECT outcome_json FROM agent_action WHERE id = ?")
            .bind(action.id)
            .fetch_one(db.pool())
            .await
            .expect("action outcome");
    assert_eq!(action_outcome.as_deref(), Some(outcome));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_action_execution WHERE id = ?",)
            .bind(execution_id)
            .fetch_one(db.pool())
            .await
            .expect("action execution linkage"),
        1
    );

    let replay = SharedMediaRepo::create_project_media_attachment_mutation(
        &db,
        mutation(
            attachment("evidence-action", "agent", identity_id, &now),
            "evidence-action-key",
            "evidence-action-domain-fingerprint",
            Some(receipt_for_replay.clone()),
            Some(execution_for_replay),
        ),
    )
    .await
    .expect("action-backed evidence replay");
    assert_eq!(replay, committed);

    let changed_execution = CreateAgentActionExecution {
        id: execution_id.to_owned(),
        action_id: "evidence-agent-action".to_owned(),
        expected_action_version: 1,
        attempt: 1,
        status: db::AgentActionExecutionStatus::Succeeded,
        result_json: Some(r#"{"changed":true}"#.to_owned()),
        error: None,
        executed_by_type: "agent".to_owned(),
        executed_by_id: identity_id.to_owned(),
        idempotency_key: "evidence-action-key".to_owned(),
        action_status: AgentActionStatus::Executed,
        action_outcome_json: Some(r#"{"changed":true}"#.to_owned()),
        created_at: now.clone(),
        completed_at: Some(now.clone()),
        updated_at: now,
    };
    assert!(matches!(
        SharedMediaRepo::create_project_media_attachment_mutation(
            &db,
            mutation(
                attachment("evidence-action", "agent", identity_id, &now_rfc3339(),),
                "evidence-action-key",
                "evidence-action-domain-fingerprint",
                Some(receipt_for_replay),
                Some(changed_execution),
            ),
        )
        .await,
        Err(DbError::IdempotencyConflict)
    ));
}
