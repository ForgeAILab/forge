use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations,
    ApplyProjectExecutionSetupCommand, CommandReceiptRepo, CreateCommandReceipt, CreateProject,
    DbError, ProjectExecutionSetupCommandRepo, ProjectProvisioningRepo, ProjectRepo,
    ReconcileProjectProvisioningCheckpoint, ReconcileProjectProvisioningMetadata, SqliteDb,
};

async fn database() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    SqliteDb::new(pool)
}

#[tokio::test]
async fn setup_version_conflict_rolls_back_checkpoint_metadata_and_receipt() {
    let db = database().await;
    let project_id = new_uuid_v4();
    let now = now_rfc3339();
    let project = ProjectRepo::create(
        &db,
        CreateProject {
            id: project_id.clone(),
            name: "Setup rollback test".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some("setup-user".to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("project creates");
    let operation = ProjectProvisioningRepo::get_provisioning_operation(&db, &project.id)
        .await
        .expect("operation loads")
        .expect("Project creation creates operation");
    let checkpoint =
        ProjectProvisioningRepo::get_provisioning_checkpoint(&db, &operation.id, "preflight")
            .await
            .expect("checkpoint loads")
            .expect("preflight checkpoint exists");
    let before_project = ProjectRepo::get_by_id(&db, &project.id)
        .await
        .expect("Project reloads")
        .expect("Project remains present");

    let mut command_receipt = receipt("setup-rollback-receipt", "rollback-digest");
    command_receipt.scope_id = project.id.clone();
    command_receipt.operation = "project.execution_setup.rollback".to_owned();
    command_receipt.idempotency_key = "setup-rollback-key".to_owned();
    let result = ProjectExecutionSetupCommandRepo::apply_project_execution_setup_command(
        &db,
        ApplyProjectExecutionSetupCommand {
            project_id: project.id.clone(),
            // Force the Project CAS to fail before any checkpoint update.
            expected_project_version: Some(project.version - 1),
            settings: Some(r#"{"should_not_commit":true}"#.to_owned()),
            primary_repo_id: None,
            bump_project_version: true,
            provisioning_retry: None,
            provisioning_metadata: Some(ReconcileProjectProvisioningMetadata {
                operation_id: operation.id.clone(),
                expected_version: operation.version,
                status: "ready".to_owned(),
                current_checkpoint: "completed".to_owned(),
                retryable: false,
                completed_at: Some(now_rfc3339()),
                updated_at: now_rfc3339(),
                checkpoints: vec![ReconcileProjectProvisioningCheckpoint {
                    id: checkpoint.id.clone(),
                    operation_id: operation.id.clone(),
                    checkpoint: checkpoint.checkpoint.clone(),
                    status: "completed".to_owned(),
                    attempt_count: checkpoint.attempt_count,
                    details_json: r#"{"uncommitted":true}"#.to_owned(),
                    started_at: checkpoint.started_at.clone(),
                    completed_at: Some(now_rfc3339()),
                    created_at: checkpoint.created_at.clone(),
                    expected_version: checkpoint.version,
                }],
            }),
            receipt: command_receipt,
        },
    )
    .await;
    assert!(matches!(result, Err(DbError::VersionConflict)));

    let after_project = ProjectRepo::get_by_id(&db, &project.id)
        .await
        .expect("Project reloads after rollback")
        .expect("Project remains present after rollback");
    assert_eq!(after_project.version, before_project.version);
    assert_eq!(after_project.settings, before_project.settings);
    let after_checkpoint =
        ProjectProvisioningRepo::get_provisioning_checkpoint(&db, &operation.id, "preflight")
            .await
            .expect("checkpoint reloads after rollback")
            .expect("preflight checkpoint remains present");
    assert_eq!(after_checkpoint.version, checkpoint.version);
    assert_eq!(after_checkpoint.status, checkpoint.status);
    assert_eq!(after_checkpoint.details_json, checkpoint.details_json);
    let after_operation = ProjectProvisioningRepo::get_provisioning_operation(&db, &project.id)
        .await
        .expect("operation reloads after rollback")
        .expect("operation remains present after rollback");
    assert_eq!(after_operation.version, operation.version);
    let receipt_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM command_receipt
         WHERE scope_type = 'project' AND scope_id = ?
           AND operation = 'project.execution_setup.rollback'",
    )
    .bind(&project.id)
    .fetch_one(db.pool())
    .await
    .expect("receipt count reads");
    assert_eq!(receipt_count, 0);
    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event
         WHERE scope_type = 'project' AND scope_id = ?
           AND event_type = 'project.execution_setup.command_committed'",
    )
    .bind(&project.id)
    .fetch_one(db.pool())
    .await
    .expect("event count reads");
    assert_eq!(event_count, 0);
}

fn receipt(id: &str, digest: &str) -> CreateCommandReceipt {
    let now = now_rfc3339();
    CreateCommandReceipt {
        id: id.to_owned(),
        principal_type: "user".to_owned(),
        principal_id: "setup-user".to_owned(),
        scope_type: "project".to_owned(),
        scope_id: String::new(),
        operation: "project.execution_setup.test".to_owned(),
        idempotency_key: "setup-key".to_owned(),
        input_digest: digest.to_owned(),
        policy_result: "allowed".to_owned(),
        correlation_id: new_uuid_v4(),
        causation_id: None,
        causation_depth: 0,
        event_id: new_uuid_v4(),
        agent_action_execution_id: None,
        outcome_json: r#"{"accepted":true}"#.to_owned(),
        committed_at: now,
    }
}

#[tokio::test]
async fn setup_project_cas_and_receipt_replay_are_one_atomic_boundary() {
    let db = database().await;
    let project_id = new_uuid_v4();
    let now = now_rfc3339();
    let project = ProjectRepo::create(
        &db,
        CreateProject {
            id: project_id.clone(),
            name: "Setup command test".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some("setup-user".to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("project creates");

    let mut first_receipt = receipt("setup-receipt-1", "digest-a");
    first_receipt.scope_id = project_id.clone();
    let applied = ProjectExecutionSetupCommandRepo::apply_project_execution_setup_command(
        &db,
        ApplyProjectExecutionSetupCommand {
            project_id: project_id.clone(),
            expected_project_version: Some(project.version),
            settings: Some(r#"{"selected":true}"#.to_owned()),
            primary_repo_id: None,
            bump_project_version: true,
            provisioning_retry: None,
            provisioning_metadata: None,
            receipt: first_receipt,
        },
    )
    .await
    .expect("Project CAS and receipt commit");
    assert!(!applied.replayed);
    assert_eq!(applied.project.version, project.version + 1);

    let mut replay_receipt = receipt("setup-receipt-2", "digest-a");
    replay_receipt.scope_id = project_id.clone();
    let replay = ProjectExecutionSetupCommandRepo::apply_project_execution_setup_command(
        &db,
        ApplyProjectExecutionSetupCommand {
            project_id: project_id.clone(),
            expected_project_version: Some(project.version),
            settings: Some(r#"{"different":true}"#.to_owned()),
            primary_repo_id: None,
            bump_project_version: true,
            provisioning_retry: None,
            provisioning_metadata: None,
            receipt: replay_receipt,
        },
    )
    .await
    .expect("same receipt replays without applying stale CAS");
    assert!(replay.replayed);
    assert_eq!(replay.project.version, project.version + 1);
    assert_eq!(replay.project.settings, r#"{"selected":true}"#);

    let changed = CommandReceiptRepo::get_command_receipt(
        &db,
        "user",
        "setup-user",
        "project",
        &project_id,
        "project.execution_setup.test",
        "setup-key",
        "digest-b",
    )
    .await;
    assert!(matches!(changed, Err(DbError::IdempotencyConflict)));
}
