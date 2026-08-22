use db::{
    create_sqlite_pool, now_rfc3339, run_migrations, AgentActionApprovalDecision,
    AgentActionExecutionStatus, AgentActionPolicyResult, AgentActionRepo, AgentActionStatus,
    AgentCommitmentRepo, AgentCommitmentStatus, AgentInboxKind, AgentInboxRepo, AgentInboxStatus,
    AgentRepo, AgentStatus, CreateAgent, CreateAgentAction, CreateAgentActionApproval,
    CreateAgentActionExecution, CreateAgentCommitment, CreateAgentCommitmentEvidence,
    CreateAgentInboxItem, CreateAgentQuestion, DbError, SqliteDb, TransferAgentCommitment,
};

async fn sqlite_db() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    let db = SqliteDb::new(pool);
    for id in ["agent-a", "agent-b"] {
        AgentRepo::create(
            &db,
            CreateAgent {
                id: id.to_owned(),
                name: id.to_owned(),
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
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .expect("agent creates");
    }
    db
}

#[tokio::test]
async fn commitment_repository_completion_is_evidence_backed_and_versioned() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let commitment = AgentCommitmentRepo::create_commitment(
        &db,
        CreateAgentCommitment {
            id: "commitment-1".to_owned(),
            owner_identity_id: "agent-a".to_owned(),
            scope_type: "account".to_owned(),
            scope_id: "account-1".to_owned(),
            title: "Deliver".to_owned(),
            description: None,
            status: AgentCommitmentStatus::Open,
            due_at: None,
            correlation_id: "correlation-1".to_owned(),
            originating_action_id: None,
            originating_task_id: None,
            evidence_required: true,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("commitment creates");

    let evidence = CreateAgentCommitmentEvidence {
        id: "evidence-1".to_owned(),
        commitment_id: commitment.id.clone(),
        evidence_type: "task_delivery".to_owned(),
        evidence_id: "task-1".to_owned(),
        scope_type: "account".to_owned(),
        scope_id: "account-1".to_owned(),
        description: None,
        metadata_json: "{}".to_owned(),
        authorized_by_type: "forge".to_owned(),
        authorized_by_id: "event-1".to_owned(),
        dedupe_key: "delivery-1".to_owned(),
        created_at: now.clone(),
    };
    let completed = AgentCommitmentRepo::complete_commitment(
        &db,
        db::CompleteAgentCommitment {
            id: commitment.id.clone(),
            expected_version: commitment.version,
            evidence: evidence.clone(),
            actor_type: "forge".to_owned(),
            actor_id: "event-1".to_owned(),
            reason: None,
            dedupe_key: "completion-1".to_owned(),
            completed_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("completion succeeds");
    assert_eq!(completed.status, AgentCommitmentStatus::Completed);
    assert!(completed.version > commitment.version);
    assert_eq!(
        AgentCommitmentRepo::list_commitment_evidence(&db, &commitment.id)
            .await
            .expect("evidence list")
            .len(),
        1
    );

    let stale = AgentCommitmentRepo::update_commitment(
        &db,
        db::UpdateAgentCommitment {
            id: commitment.id,
            expected_version: 1,
            status: Some(AgentCommitmentStatus::Blocked),
            due_at: None,
            description: None,
            blocked_reason: Some(Some("late".to_owned())),
            cancellation_reason: None,
            actor_type: "agent".to_owned(),
            actor_id: "agent-a".to_owned(),
            reason: Some("stale".to_owned()),
            evidence_id: None,
            dedupe_key: "stale-1".to_owned(),
            updated_at: now,
        },
    )
    .await;
    assert!(matches!(stale, Err(DbError::VersionConflict)));
}

#[tokio::test]
async fn commitment_transfer_is_explicit_auditable_and_idempotent() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let commitment = AgentCommitmentRepo::create_commitment(
        &db,
        CreateAgentCommitment {
            id: "commitment-transfer".to_owned(),
            owner_identity_id: "agent-a".to_owned(),
            scope_type: "project".to_owned(),
            scope_id: "project-transfer".to_owned(),
            title: "Carry the delivery obligation".to_owned(),
            description: None,
            status: AgentCommitmentStatus::InProgress,
            due_at: None,
            correlation_id: "correlation-transfer".to_owned(),
            originating_action_id: None,
            originating_task_id: None,
            evidence_required: true,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("commitment creates");

    let replacement = AgentCommitmentRepo::transfer_commitment(
        &db,
        TransferAgentCommitment {
            id: commitment.id.clone(),
            expected_version: commitment.version,
            to_identity_id: "agent-b".to_owned(),
            reason: "Project Agent binding replaced".to_owned(),
            actor_type: "user".to_owned(),
            actor_id: "user-1".to_owned(),
            dedupe_key: "binding-transfer-1".to_owned(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("explicit transfer succeeds");
    assert_eq!(replacement.owner_identity_id, "agent-b");
    assert_eq!(replacement.version, commitment.version + 1);

    // A replay may carry the stale pre-transfer version, but the same
    // durable dedupe key returns the original outcome without another
    // transfer or version increment.
    let replay = AgentCommitmentRepo::transfer_commitment(
        &db,
        TransferAgentCommitment {
            id: commitment.id.clone(),
            expected_version: commitment.version,
            to_identity_id: "agent-b".to_owned(),
            reason: "Project Agent binding replaced".to_owned(),
            actor_type: "user".to_owned(),
            actor_id: "user-1".to_owned(),
            dedupe_key: "binding-transfer-1".to_owned(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("transfer replay returns original outcome");
    assert_eq!(replay.id, replacement.id);
    assert_eq!(replay.owner_identity_id, "agent-b");
    assert_eq!(replay.version, replacement.version);

    let transfers = AgentCommitmentRepo::list_commitment_transfers(&db, &commitment.id)
        .await
        .expect("transfer history loads");
    assert_eq!(transfers.len(), 1);
    assert_eq!(transfers[0].from_identity_id, "agent-a");
    assert_eq!(transfers[0].to_identity_id, "agent-b");
    assert_eq!(transfers[0].reason, "Project Agent binding replaced");

    let lifecycle = AgentCommitmentRepo::list_commitment_lifecycle(&db, &commitment.id)
        .await
        .expect("commitment lifecycle loads");
    assert_eq!(lifecycle.len(), 1);
    assert_eq!(lifecycle[0].dedupe_key, "binding-transfer-1");

    let stale = AgentCommitmentRepo::transfer_commitment(
        &db,
        TransferAgentCommitment {
            id: commitment.id.clone(),
            expected_version: commitment.version,
            to_identity_id: "agent-a".to_owned(),
            reason: "stale replacement attempt".to_owned(),
            actor_type: "user".to_owned(),
            actor_id: "user-1".to_owned(),
            dedupe_key: "binding-transfer-stale".to_owned(),
            updated_at: now,
        },
    )
    .await;
    assert!(matches!(stale, Err(DbError::VersionConflict)));
}

#[tokio::test]
async fn action_and_inbox_replays_are_idempotent() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let action = AgentActionRepo::create_action(
        &db,
        CreateAgentAction {
            id: "action-1".to_owned(),
            actor_identity_id: "agent-a".to_owned(),
            scope_type: "account".to_owned(),
            scope_id: "account-1".to_owned(),
            operation: "task.propose".to_owned(),
            payload_json: "{}".to_owned(),
            payload_hash: "hash".to_owned(),
            dedupe_key: "proposal-1".to_owned(),
            correlation_id: "correlation-1".to_owned(),
            causation_id: None,
            causation_depth: 0,
            requested_permission: "task:create".to_owned(),
            policy_result: AgentActionPolicyResult::ApprovalRequired,
            policy_reason: None,
            status: AgentActionStatus::PendingApproval,
            target_type: Some("project".to_owned()),
            target_id: Some("project-1".to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("action creates");
    let replay = AgentActionRepo::create_action(
        &db,
        CreateAgentAction {
            id: "different-id".to_owned(),
            actor_identity_id: "agent-a".to_owned(),
            scope_type: "account".to_owned(),
            scope_id: "account-1".to_owned(),
            operation: "task.propose".to_owned(),
            payload_json: "{}".to_owned(),
            payload_hash: "hash".to_owned(),
            dedupe_key: "proposal-1".to_owned(),
            correlation_id: "correlation-1".to_owned(),
            causation_id: None,
            causation_depth: 0,
            requested_permission: "task:create".to_owned(),
            policy_result: AgentActionPolicyResult::ApprovalRequired,
            policy_reason: None,
            status: AgentActionStatus::PendingApproval,
            target_type: Some("project".to_owned()),
            target_id: Some("project-1".to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("action replay");
    assert_eq!(action.id, replay.id);

    AgentActionRepo::record_action_approval(
        &db,
        CreateAgentActionApproval {
            id: "approval-1".to_owned(),
            action_id: action.id.clone(),
            expected_action_version: action.version,
            approver_identity_id: "agent-b".to_owned(),
            decision: AgentActionApprovalDecision::Approved,
            reason: None,
            resulting_status: AgentActionStatus::Approved,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("approval");
    let current = AgentActionRepo::get_action(&db, &action.id)
        .await
        .expect("action loads")
        .expect("action exists");
    let execution = AgentActionRepo::record_action_execution(
        &db,
        CreateAgentActionExecution {
            id: "execution-1".to_owned(),
            action_id: action.id.clone(),
            expected_action_version: current.version,
            attempt: 1,
            status: AgentActionExecutionStatus::Succeeded,
            result_json: Some("{}".to_owned()),
            error: None,
            executed_by_type: "forge".to_owned(),
            executed_by_id: "forge".to_owned(),
            idempotency_key: "execute-1".to_owned(),
            action_status: AgentActionStatus::Executed,
            action_outcome_json: Some("{}".to_owned()),
            created_at: now.clone(),
            completed_at: Some(now.clone()),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("execution");
    let execution_replay = AgentActionRepo::record_action_execution(
        &db,
        CreateAgentActionExecution {
            id: "different-execution".to_owned(),
            action_id: action.id,
            expected_action_version: current.version,
            attempt: 1,
            status: AgentActionExecutionStatus::Succeeded,
            result_json: Some("{}".to_owned()),
            error: None,
            executed_by_type: "forge".to_owned(),
            executed_by_id: "forge".to_owned(),
            idempotency_key: "execute-1".to_owned(),
            action_status: AgentActionStatus::Executed,
            action_outcome_json: Some("{}".to_owned()),
            created_at: now.clone(),
            completed_at: Some(now.clone()),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("execution replay");
    assert_eq!(execution.id, execution_replay.id);

    let item = AgentInboxRepo::create_inbox_item(
        &db,
        CreateAgentInboxItem {
            id: "inbox-1".to_owned(),
            recipient_identity_id: "agent-a".to_owned(),
            scope_type: "account".to_owned(),
            scope_id: "account-1".to_owned(),
            kind: AgentInboxKind::TaskOutcome,
            status: AgentInboxStatus::Unread,
            title: "Outcome".to_owned(),
            body: "done".to_owned(),
            payload_json: "{}".to_owned(),
            source_type: Some("task".to_owned()),
            source_id: Some("task-1".to_owned()),
            correlation_id: "correlation-1".to_owned(),
            causation_id: None,
            dedupe_key: "outcome-1".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("inbox creates");
    let item_replay = AgentInboxRepo::create_inbox_item(
        &db,
        CreateAgentInboxItem {
            id: "different-inbox".to_owned(),
            recipient_identity_id: "agent-a".to_owned(),
            scope_type: "account".to_owned(),
            scope_id: "account-1".to_owned(),
            kind: AgentInboxKind::TaskOutcome,
            status: AgentInboxStatus::Unread,
            title: "Outcome".to_owned(),
            body: "done".to_owned(),
            payload_json: "{}".to_owned(),
            source_type: Some("task".to_owned()),
            source_id: Some("task-1".to_owned()),
            correlation_id: "correlation-1".to_owned(),
            causation_id: None,
            dedupe_key: "outcome-1".to_owned(),
            created_at: now,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("inbox replay");
    assert_eq!(item.id, item_replay.id);
}

#[tokio::test]
async fn question_and_inbox_are_committed_atomically_and_replay_safely() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let inbox = CreateAgentInboxItem {
        id: "question-inbox-1".to_owned(),
        recipient_identity_id: "agent-a".to_owned(),
        scope_type: "account".to_owned(),
        scope_id: "account-1".to_owned(),
        kind: AgentInboxKind::Question,
        status: AgentInboxStatus::Unread,
        title: "Need input".to_owned(),
        body: "Which branch?".to_owned(),
        payload_json: r#"{"choices":["main"]}"#.to_owned(),
        source_type: Some("question".to_owned()),
        source_id: Some("question-1".to_owned()),
        correlation_id: "correlation-question".to_owned(),
        causation_id: None,
        dedupe_key: "question-delivery-1".to_owned(),
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    let question = CreateAgentQuestion {
        id: "question-1".to_owned(),
        recipient_identity_id: "agent-a".to_owned(),
        scope_type: "account".to_owned(),
        scope_id: "account-1".to_owned(),
        question: "Which branch?".to_owned(),
        context_json: inbox.payload_json.clone(),
        asked_by_type: "forge".to_owned(),
        asked_by_id: "forge".to_owned(),
        inbox_item_id: Some(inbox.id.clone()),
        due_at: None,
        correlation_id: inbox.correlation_id.clone(),
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    let first = AgentInboxRepo::create_question_with_inbox(&db, inbox.clone(), question.clone())
        .await
        .expect("question creates");
    let replay = AgentInboxRepo::create_question_with_inbox(&db, inbox, question)
        .await
        .expect("question replay");
    assert_eq!(first.id, replay.id);
    let inbox_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_inbox_item")
        .fetch_one(db.pool())
        .await
        .expect("inbox count");
    let question_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_question")
        .fetch_one(db.pool())
        .await
        .expect("question count");
    assert_eq!(inbox_count, 1);
    assert_eq!(question_count, 1);
}
