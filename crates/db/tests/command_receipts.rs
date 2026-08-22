use db::{
    begin_immediate, create_sqlite_pool, now_rfc3339, run_migrations, AgentActionRepo,
    AgentActionStatus, AgentRepo, AgentStatus, CommandReceiptRepo, CreateAgent, CreateAgentAction,
    CreateAgentActionExecution, CreateCommandReceipt, CreateDomainEvent, DbError, DomainEventRepo,
    SqliteDb,
};

async fn database() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    SqliteDb::new(pool)
}

async fn event(db: &SqliteDb, id: &str) -> db::DomainEvent {
    let now = now_rfc3339();
    DomainEventRepo::append_event(
        db,
        CreateDomainEvent {
            id: id.to_owned(),
            event_type: "command.test_committed".to_owned(),
            entity_type: "command".to_owned(),
            entity_id: id.to_owned(),
            actor_type: "user".to_owned(),
            actor_id: Some("user-1".to_owned()),
            scope_type: "account".to_owned(),
            scope_id: "account-1".to_owned(),
            correlation_id: format!("correlation-{id}"),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!("command-event-{id}")),
            payload_json: "{}".to_owned(),
            created_at: now,
        },
    )
    .await
    .expect("event persists")
}

fn receipt(event_id: &str, id: &str, digest: &str) -> CreateCommandReceipt {
    CreateCommandReceipt {
        id: id.to_owned(),
        principal_type: "user".to_owned(),
        principal_id: "user-1".to_owned(),
        scope_type: "account".to_owned(),
        scope_id: "account-1".to_owned(),
        operation: "command.test".to_owned(),
        idempotency_key: "same-key".to_owned(),
        input_digest: digest.to_owned(),
        policy_result: "allowed".to_owned(),
        correlation_id: "correlation-command".to_owned(),
        causation_id: None,
        causation_depth: 0,
        event_id: event_id.to_owned(),
        agent_action_execution_id: None,
        outcome_json: format!(r#"{{"result_id":"{id}"}}"#),
        committed_at: now_rfc3339(),
    }
}

#[tokio::test]
async fn command_receipt_replay_is_frozen_and_digest_bound() {
    let db = database().await;
    let committed_event = event(&db, "event-receipt-1").await;
    let original = CommandReceiptRepo::create_command_receipt(
        &db,
        receipt(&committed_event.id, "receipt-1", "digest-a"),
    )
    .await
    .expect("receipt persists");

    let replay = CommandReceiptRepo::get_command_receipt(
        &db,
        "user",
        "user-1",
        "account",
        "account-1",
        "command.test",
        "same-key",
        "digest-a",
    )
    .await
    .expect("replay lookup")
    .expect("receipt exists");
    assert_eq!(replay, original);

    let same_digest_retry = CommandReceiptRepo::create_command_receipt(
        &db,
        receipt(&committed_event.id, "different-id", "digest-a"),
    )
    .await
    .expect("same digest returns frozen receipt");
    assert_eq!(same_digest_retry, original);

    let changed_digest = CommandReceiptRepo::get_command_receipt(
        &db,
        "user",
        "user-1",
        "account",
        "account-1",
        "command.test",
        "same-key",
        "digest-b",
    )
    .await;
    assert!(matches!(changed_digest, Err(DbError::IdempotencyConflict)));

    let changed_principal = CommandReceiptRepo::get_command_receipt(
        &db,
        "agent",
        "agent-1",
        "account",
        "account-1",
        "command.test",
        "same-key",
        "digest-a",
    )
    .await;
    assert!(matches!(
        changed_principal,
        Err(DbError::IdempotencyConflict)
    ));

    let mut changed_create = receipt(&committed_event.id, "different-id", "digest-a");
    changed_create.principal_id = "user-2".to_owned();
    let changed_create = CommandReceiptRepo::create_command_receipt(&db, changed_create).await;
    assert!(matches!(changed_create, Err(DbError::IdempotencyConflict)));
}

#[tokio::test]
async fn command_receipts_are_immutable_and_require_provenance_foreign_keys() {
    let db = database().await;
    let committed_event = event(&db, "event-receipt-immutable").await;
    let original = CommandReceiptRepo::create_command_receipt(
        &db,
        receipt(&committed_event.id, "receipt-immutable", "digest-immutable"),
    )
    .await
    .expect("receipt persists");

    let update =
        sqlx::query("UPDATE command_receipt SET outcome_json = '{\"tampered\":true}' WHERE id = ?")
            .bind(&original.id)
            .execute(db.pool())
            .await;
    assert!(update.is_err(), "receipt update must be rejected");

    let delete = sqlx::query("DELETE FROM command_receipt WHERE id = ?")
        .bind(&original.id)
        .execute(db.pool())
        .await;
    assert!(delete.is_err(), "receipt delete must be rejected");

    let loaded = CommandReceiptRepo::get_command_receipt(
        &db,
        &original.principal_type,
        &original.principal_id,
        &original.scope_type,
        &original.scope_id,
        &original.operation,
        &original.idempotency_key,
        &original.input_digest,
    )
    .await
    .expect("receipt lookup")
    .expect("receipt remains present");
    assert_eq!(loaded, original);

    let mut missing_event = receipt(
        "event-does-not-exist",
        "receipt-missing-event",
        "digest-missing-event",
    );
    missing_event.operation = "command.missing_event".to_owned();
    missing_event.idempotency_key = "missing-event-key".to_owned();
    let missing_event = CommandReceiptRepo::create_command_receipt(&db, missing_event).await;
    assert!(
        !matches!(missing_event, Err(DbError::IdempotencyConflict)),
        "missing event must not be classified as an idempotency replay: {missing_event:?}"
    );

    let mut missing_action = receipt(
        &committed_event.id,
        "receipt-missing-action",
        "digest-missing-action",
    );
    missing_action.operation = "command.missing_action".to_owned();
    missing_action.idempotency_key = "missing-action-key".to_owned();
    missing_action.agent_action_execution_id = Some("execution-does-not-exist".to_owned());
    let missing_action = CommandReceiptRepo::create_command_receipt(&db, missing_action).await;
    assert!(
        !matches!(missing_action, Err(DbError::IdempotencyConflict)),
        "missing action execution must not be classified as an idempotency replay: {missing_action:?}"
    );
}

#[tokio::test]
async fn command_receipt_primary_key_collision_is_not_an_idempotency_replay() {
    let db = database().await;
    let committed_event = event(&db, "event-receipt-primary-key").await;
    let original = CommandReceiptRepo::create_command_receipt(
        &db,
        receipt(&committed_event.id, "receipt-primary-key", "digest-a"),
    )
    .await
    .expect("receipt persists");

    let mut primary_key_collision = receipt(
        &committed_event.id,
        &original.id,
        "digest-different-command",
    );
    primary_key_collision.operation = "command.other".to_owned();
    primary_key_collision.idempotency_key = "other-key".to_owned();
    let result = CommandReceiptRepo::create_command_receipt(&db, primary_key_collision).await;
    assert!(
        !matches!(result, Err(DbError::IdempotencyConflict)),
        "primary-key collision must not be reported as idempotency conflict: {result:?}"
    );
    assert!(matches!(result, Err(DbError::Sqlx(_))));
}

async fn seed_agent_and_action(db: &SqliteDb, action_id: &str) -> db::AgentAction {
    let now = now_rfc3339();
    AgentRepo::create(
        db,
        CreateAgent {
            id: "action-agent".to_owned(),
            name: "Action Agent".to_owned(),
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
            status: AgentStatus::Idle,
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
    .expect("identity persists");

    AgentActionRepo::create_action(
        db,
        CreateAgentAction {
            id: action_id.to_owned(),
            actor_identity_id: "action-agent".to_owned(),
            scope_type: "account".to_owned(),
            scope_id: "account-1".to_owned(),
            operation: "command.test".to_owned(),
            payload_json: "{}".to_owned(),
            payload_hash: "hash".to_owned(),
            dedupe_key: format!("dedupe-{action_id}"),
            correlation_id: format!("correlation-{action_id}"),
            causation_id: None,
            causation_depth: 0,
            requested_permission: "command:test".to_owned(),
            policy_result: db::AgentActionPolicyResult::ApprovalRequired,
            policy_reason: None,
            status: AgentActionStatus::Approved,
            target_type: Some("command".to_owned()),
            target_id: Some(action_id.to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("action persists")
}

fn action_execution(action: &db::AgentAction, id: &str, key: &str) -> CreateAgentActionExecution {
    let now = now_rfc3339();
    CreateAgentActionExecution {
        id: id.to_owned(),
        action_id: action.id.clone(),
        expected_action_version: action.version,
        attempt: 1,
        status: db::AgentActionExecutionStatus::Succeeded,
        result_json: Some(r#"{"ok":true}"#.to_owned()),
        error: None,
        executed_by_type: "system".to_owned(),
        executed_by_id: "forge".to_owned(),
        idempotency_key: key.to_owned(),
        action_status: AgentActionStatus::Executed,
        action_outcome_json: Some(r#"{"ok":true}"#.to_owned()),
        created_at: now.clone(),
        completed_at: Some(now),
        updated_at: now_rfc3339(),
    }
}

#[tokio::test]
async fn action_completion_is_cas_and_replay_exact_inside_transaction() {
    let db = database().await;
    let action = seed_agent_and_action(&db, "action-cas-1").await;
    let mut tx = begin_immediate(db.pool()).await.expect("transaction");
    let first = AgentActionRepo::record_action_execution_in_tx(
        &db,
        &mut tx,
        action_execution(&action, "execution-1", "execution-key"),
    )
    .await
    .expect("completion persists");
    tx.commit().await.expect("completion commits");

    let current = AgentActionRepo::get_action(&db, &action.id)
        .await
        .expect("action lookup")
        .expect("action exists");
    assert_eq!(current.status, AgentActionStatus::Executed);
    assert_eq!(current.version, action.version + 1);

    let mut replay_input = action_execution(&current, "different-execution-id", "execution-key");
    replay_input.expected_action_version = action.version;
    let mut replay_tx = begin_immediate(db.pool())
        .await
        .expect("replay transaction");
    let replay = AgentActionRepo::record_action_execution_in_tx(&db, &mut replay_tx, replay_input)
        .await
        .expect("same execution replays");
    replay_tx.commit().await.expect("replay commits");
    assert_eq!(replay, first);

    let mut stale_input = action_execution(&current, "execution-stale", "execution-key-2");
    stale_input.expected_action_version = action.version;
    let mut stale_tx = begin_immediate(db.pool()).await.expect("stale transaction");
    let stale =
        AgentActionRepo::record_action_execution_in_tx(&db, &mut stale_tx, stale_input).await;
    assert!(matches!(stale, Err(DbError::VersionConflict)));
    stale_tx.rollback().await.expect("stale rolls back");
}

#[tokio::test]
async fn action_replay_compares_execution_and_action_outcome_inputs() {
    let db = database().await;
    let action = seed_agent_and_action(&db, "action-replay-inputs").await;
    let first = AgentActionRepo::record_action_execution(
        &db,
        action_execution(&action, "execution-replay-inputs", "execution-replay-key"),
    )
    .await
    .expect("completion persists");
    assert_eq!(first.status, db::AgentActionExecutionStatus::Succeeded);

    let current = AgentActionRepo::get_action(&db, &action.id)
        .await
        .expect("action lookup")
        .expect("action exists");

    let mut changed_result =
        action_execution(&current, "different-execution-id", "execution-replay-key");
    changed_result.expected_action_version = 1;
    changed_result.result_json = Some(r#"{"ok":false}"#.to_owned());
    let changed_result = AgentActionRepo::record_action_execution(&db, changed_result).await;
    assert!(matches!(changed_result, Err(DbError::IdempotencyConflict)));

    let mut changed_status = action_execution(
        &current,
        "different-execution-status",
        "execution-replay-key",
    );
    changed_status.status = db::AgentActionExecutionStatus::Failed;
    changed_status.error = Some("different failure".to_owned());
    let changed_status = AgentActionRepo::record_action_execution(&db, changed_status).await;
    assert!(matches!(changed_status, Err(DbError::IdempotencyConflict)));

    let mut changed_action_outcome = action_execution(
        &current,
        "different-execution-action-outcome",
        "execution-replay-key",
    );
    changed_action_outcome.action_status = AgentActionStatus::Failed;
    changed_action_outcome.action_outcome_json = Some(r#"{"ok":false}"#.to_owned());
    let changed_action_outcome =
        AgentActionRepo::record_action_execution(&db, changed_action_outcome).await;
    assert!(matches!(
        changed_action_outcome,
        Err(DbError::IdempotencyConflict)
    ));

    // A different idempotency key with the same attempt collides with the
    // attempt uniqueness constraint, not the replay key.  It must remain a
    // database constraint error so callers can distinguish the two cases.
    let mut changed_key_same_attempt = action_execution(
        &current,
        "different-execution-attempt",
        "different-replay-key",
    );
    changed_key_same_attempt.expected_action_version = current.version;
    let changed_key_same_attempt =
        AgentActionRepo::record_action_execution(&db, changed_key_same_attempt).await;
    assert!(
        !matches!(changed_key_same_attempt, Err(DbError::IdempotencyConflict)),
        "attempt collision must not be classified as idempotency conflict: {changed_key_same_attempt:?}"
    );
    assert!(matches!(changed_key_same_attempt, Err(DbError::Sqlx(_))));
}

#[tokio::test]
async fn receipt_event_action_and_domain_writes_rollback_together() {
    let db = database().await;
    let action = seed_agent_and_action(&db, "action-rollback-1").await;
    let mut tx = begin_immediate(db.pool()).await.expect("transaction");
    let committed_event = DomainEventRepo::append_event_in_tx(
        &db,
        &mut tx,
        &CreateDomainEvent {
            id: "event-rollback-1".to_owned(),
            event_type: "command.rollback".to_owned(),
            entity_type: "command".to_owned(),
            entity_id: "rollback-command".to_owned(),
            actor_type: "agent".to_owned(),
            actor_id: Some(action.actor_identity_id.clone()),
            scope_type: "account".to_owned(),
            scope_id: "account-1".to_owned(),
            correlation_id: "correlation-rollback".to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("event-rollback-key".to_owned()),
            payload_json: "{}".to_owned(),
            created_at: now_rfc3339(),
        },
    )
    .await
    .expect("event prepares");
    let execution = AgentActionRepo::record_action_execution_in_tx(
        &db,
        &mut tx,
        action_execution(&action, "execution-rollback", "execution-rollback-key"),
    )
    .await
    .expect("action prepares");
    let mut command_receipt = receipt(&committed_event.id, "receipt-rollback", "digest-rollback");
    command_receipt.agent_action_execution_id = Some(execution.id);
    CommandReceiptRepo::create_command_receipt_in_tx(&db, &mut tx, command_receipt)
        .await
        .expect("receipt prepares");
    tx.rollback().await.expect("all command writes roll back");

    assert!(CommandReceiptRepo::get_command_receipt(
        &db,
        "user",
        "user-1",
        "account",
        "account-1",
        "command.test",
        "same-key",
        "digest-rollback",
    )
    .await
    .expect("receipt lookup")
    .is_none());
    assert!(DomainEventRepo::get_event(&db, "event-rollback-1")
        .await
        .expect("event lookup")
        .is_none());
    let current_action = AgentActionRepo::get_action(&db, &action.id)
        .await
        .expect("action lookup")
        .expect("action exists");
    assert_eq!(current_action.version, action.version);
    assert_eq!(current_action.status, action.status);
    assert!(AgentActionRepo::list_action_executions(&db, &action.id)
        .await
        .expect("execution list")
        .is_empty());
}
