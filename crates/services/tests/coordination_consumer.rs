use std::sync::Arc;

use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AccountMainAgentBindingRepo,
    AgentCommitmentRepo, AgentInboxRepo, AgentRepo, AgentStatus, CreateAccountMainAgentBinding,
    CreateAgentIdentity, CreateAgentProfile, CreateDomainEvent, CreateProject,
    CreateProjectAgentBinding, CreateTask, DomainEventRepo, ProjectAgentBindingRepo, ProjectRepo,
    ReplaceAccountMainAgentBinding, ReplaceProjectAgentBinding, SqliteDb, TaskRepo,
};
use services::{
    coordination_consumer_name, AttentionService, CommitmentService, CoordinationOutcomeConsumer,
    CreateCommitmentInput, TransferCommitmentInput,
};

async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    Arc::new(SqliteDb::new(pool))
}

async fn seed_identity(db: &SqliteDb, identity_id: &str) {
    let now = now_rfc3339();
    AgentRepo::create_identity_with_profile(
        db,
        CreateAgentIdentity {
            id: identity_id.to_owned(),
            name: "outcome-owner".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some("user-1".to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: new_uuid_v4(),
            identity_id: identity_id.to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("test".to_owned()),
            model: Some("test".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn task_outcome_reconciliation_replays_after_cursor_reset_without_duplicates() {
    let db = database().await;
    let identity_id = "identity-outcome";
    seed_identity(&db, identity_id).await;
    let now = now_rfc3339();
    let project_id = "project-outcome";
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.to_owned(),
            name: "Outcome Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some("user-1".to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let task = TaskRepo::create(
        &*db,
        CreateTask {
            id: "task-outcome".to_owned(),
            project_id: project_id.to_owned(),
            repo_id: None,
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "Deliver outcome".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "done".to_owned(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let commitment = CommitmentService::new(Arc::clone(&db))
        .create(CreateCommitmentInput {
            id: Some("commitment-outcome".to_owned()),
            owner_identity_id: identity_id.to_owned(),
            scope_type: "project".to_owned(),
            scope_id: project_id.to_owned(),
            title: "Deliver the proposed task".to_owned(),
            description: None,
            status: db::AgentCommitmentStatus::InProgress,
            due_at: None,
            correlation_id: "correlation-outcome".to_owned(),
            originating_action_id: None,
            originating_task_id: Some(task.id.clone()),
            evidence_required: true,
        })
        .await
        .unwrap();

    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent::task_transition(
            "task-transition-outcome",
            task.id.clone(),
            project_id,
            "in_progress",
            "done",
            None,
            "system:workflow",
            "delivery accepted",
            false,
            now,
        ),
    )
    .await
    .unwrap();

    let first = CoordinationOutcomeConsumer::new(Arc::clone(&db), "consumer-1")
        .run_once(100)
        .await
        .unwrap();
    assert!(first.claimed_events >= 1);
    assert_eq!(first.reconciled_events, 1);
    assert_eq!(first.processed_events, first.claimed_events);

    let stored = AgentCommitmentRepo::get_commitment(&*db, &commitment.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, db::AgentCommitmentStatus::Completed);
    assert_eq!(
        AgentCommitmentRepo::list_commitment_evidence(&*db, &commitment.id)
            .await
            .unwrap()
            .len(),
        1
    );
    let inbox = AgentInboxRepo::list_inbox_items(
        &*db,
        db::AgentInboxListQuery {
            recipient_identity_id: identity_id.to_owned(),
            status: None,
            scope_type: Some("project".to_owned()),
            scope_id: Some(project_id.to_owned()),
            limit: 10,
        },
    )
    .await
    .unwrap();
    assert_eq!(inbox.len(), 1);

    // Simulate a crash after the idempotent writes but before the durable
    // receipt checkpoint.  A new process/lease owner must replay the event.
    sqlx::query("DELETE FROM event_projection_receipt WHERE consumer_name = ?")
        .bind(coordination_consumer_name())
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE event_consumer_cursor SET last_sequence = 0, version = version + 1
         WHERE consumer_name = ?",
    )
    .bind(coordination_consumer_name())
    .execute(db.pool())
    .await
    .unwrap();

    let replay = CoordinationOutcomeConsumer::new(Arc::clone(&db), "consumer-2")
        .run_once(100)
        .await
        .unwrap();
    assert!(replay.claimed_events >= 1);
    assert_eq!(replay.reconciled_events, 1);
    assert_eq!(
        AgentCommitmentRepo::list_commitment_evidence(&*db, &commitment.id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        AgentInboxRepo::list_inbox_items(
            &*db,
            db::AgentInboxListQuery {
                recipient_identity_id: identity_id.to_owned(),
                status: None,
                scope_type: Some("project".to_owned()),
                scope_id: Some(project_id.to_owned()),
                limit: 10,
            },
        )
        .await
        .unwrap()
        .len(),
        1
    );
}

#[tokio::test]
async fn binding_replacement_requires_explicit_transfer_and_keeps_outcomes_with_new_owner() {
    let db = database().await;
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES ('user-1', 'continuity@example.test', 'test', NULL, ?, ?)",
    )
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .unwrap();

    let old_identity = "continuity-old";
    let new_identity = "continuity-new";
    seed_identity(&db, old_identity).await;
    seed_identity(&db, new_identity).await;
    let old_profile: String = sqlx::query_scalar(
        "SELECT id FROM agent_profile WHERE identity_id = ? ORDER BY created_at DESC LIMIT 1",
    )
    .bind(old_identity)
    .fetch_one(db.pool())
    .await
    .unwrap();
    let new_profile: String = sqlx::query_scalar(
        "SELECT id FROM agent_profile WHERE identity_id = ? ORDER BY created_at DESC LIMIT 1",
    )
    .bind(new_identity)
    .fetch_one(db.pool())
    .await
    .unwrap();

    let project_id = "continuity-project";
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.to_owned(),
            name: "Continuity Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some("user-1".to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let setup = ProjectAgentBindingRepo::get_active_project_binding(&*db, project_id)
        .await
        .unwrap()
        .unwrap();
    let first_project_binding = ProjectAgentBindingRepo::replace_project_binding(
        &*db,
        ReplaceProjectAgentBinding {
            project_id: project_id.to_owned(),
            expected_version: setup.version,
            replacement: CreateProjectAgentBinding {
                id: "continuity-project-binding-old".to_owned(),
                project_id: project_id.to_owned(),
                identity_id: Some(old_identity.to_owned()),
                profile_id: Some(old_profile.clone()),
                state: "active".to_owned(),
                autonomy_policy_json: "{}".to_owned(),
                permission_ceiling_json: "{}".to_owned(),
                subscriptions_json: "[]".to_owned(),
                wake_budget: 1,
                operating_skill_revision_id: None,
                policy_revision: "default".to_owned(),
                policy_digest: String::new(),
                charter_id: None,
                charter_revision_id: None,
                charter_setup_required: true,
                admission_receipt_id: None,
                charter_approval_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            replacement_reason: Some("initial Project Agent".to_owned()),
        },
    )
    .await
    .unwrap();

    let main_binding = AccountMainAgentBindingRepo::create_main_binding(
        &*db,
        CreateAccountMainAgentBinding {
            id: "continuity-main-binding-old".to_owned(),
            account_id: "user-1".to_owned(),
            identity_id: old_identity.to_owned(),
            profile_id: old_profile.clone(),
            autonomy_policy_json: "{}".to_owned(),
            tool_policy_revision: "continuity".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();

    let project_binding = ProjectAgentBindingRepo::replace_project_binding(
        &*db,
        ReplaceProjectAgentBinding {
            project_id: project_id.to_owned(),
            expected_version: first_project_binding.version,
            replacement: CreateProjectAgentBinding {
                id: "continuity-project-binding-new".to_owned(),
                project_id: project_id.to_owned(),
                identity_id: Some(new_identity.to_owned()),
                profile_id: Some(new_profile.clone()),
                state: "active".to_owned(),
                autonomy_policy_json: "{}".to_owned(),
                permission_ceiling_json: "{}".to_owned(),
                subscriptions_json: "[]".to_owned(),
                wake_budget: 1,
                operating_skill_revision_id: None,
                policy_revision: "default".to_owned(),
                policy_digest: String::new(),
                charter_id: None,
                charter_revision_id: None,
                charter_setup_required: true,
                admission_receipt_id: None,
                charter_approval_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            replacement_reason: Some("replace Project Agent".to_owned()),
        },
    )
    .await
    .unwrap();
    assert_eq!(project_binding.identity_id.as_deref(), Some(new_identity));

    let main_replacement = AccountMainAgentBindingRepo::replace_main_binding(
        &*db,
        ReplaceAccountMainAgentBinding {
            account_id: "user-1".to_owned(),
            expected_version: main_binding.version,
            replacement: CreateAccountMainAgentBinding {
                id: "continuity-main-binding-new".to_owned(),
                account_id: "user-1".to_owned(),
                identity_id: new_identity.to_owned(),
                profile_id: new_profile,
                autonomy_policy_json: "{}".to_owned(),
                tool_policy_revision: "continuity".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            replacement_reason: Some("replace Main Agent".to_owned()),
        },
    )
    .await
    .unwrap();
    assert_eq!(main_replacement.identity_id, new_identity);

    let task = TaskRepo::create(
        &*db,
        CreateTask {
            id: "continuity-task".to_owned(),
            project_id: project_id.to_owned(),
            repo_id: None,
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "Continuity outcome".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "in_progress".to_owned(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let commitments = CommitmentService::new(Arc::clone(&db));
    let project_commitment = commitments
        .create(CreateCommitmentInput {
            id: Some("continuity-project-commitment".to_owned()),
            owner_identity_id: old_identity.to_owned(),
            scope_type: "project".to_owned(),
            scope_id: project_id.to_owned(),
            title: "Project delivery".to_owned(),
            description: None,
            status: db::AgentCommitmentStatus::InProgress,
            due_at: None,
            correlation_id: "continuity-project-correlation".to_owned(),
            originating_action_id: None,
            originating_task_id: Some(task.id.clone()),
            evidence_required: true,
        })
        .await
        .unwrap();
    let main_commitment = commitments
        .create(CreateCommitmentInput {
            id: Some("continuity-main-commitment".to_owned()),
            owner_identity_id: old_identity.to_owned(),
            scope_type: "account".to_owned(),
            scope_id: "user-1".to_owned(),
            title: "Main delivery".to_owned(),
            description: None,
            status: db::AgentCommitmentStatus::InProgress,
            due_at: None,
            correlation_id: "continuity-main-correlation".to_owned(),
            originating_action_id: None,
            originating_task_id: Some(task.id.clone()),
            evidence_required: true,
        })
        .await
        .unwrap();

    // Replacing either binding does not silently transfer obligations or
    // expose the old identity's current focus to the replacement.
    let attention = AttentionService::new(Arc::clone(&db));
    let old_before_transfer = attention
        .agent_detail("user-1", old_identity, 10)
        .await
        .unwrap();
    let new_before_transfer = attention
        .agent_detail("user-1", new_identity, 10)
        .await
        .unwrap();
    assert_eq!(old_before_transfer.open_commitment_count, 2);
    assert_eq!(
        old_before_transfer
            .current_focus
            .as_ref()
            .map(|item| item.task_id.as_str()),
        Some(task.id.as_str())
    );
    assert_eq!(new_before_transfer.open_commitment_count, 0);
    assert!(new_before_transfer.current_focus.is_none());

    let transferred_project = commitments
        .transfer(TransferCommitmentInput {
            id: project_commitment.id.clone(),
            expected_version: project_commitment.version,
            to_identity_id: new_identity.to_owned(),
            reason: "replacement Project Agent accepts obligation".to_owned(),
            actor_type: "user".to_owned(),
            actor_id: "user-1".to_owned(),
            dedupe_key: "continuity-project-transfer".to_owned(),
        })
        .await
        .unwrap();
    let transferred_main = commitments
        .transfer(TransferCommitmentInput {
            id: main_commitment.id.clone(),
            expected_version: main_commitment.version,
            to_identity_id: new_identity.to_owned(),
            reason: "replacement Main Agent accepts obligation".to_owned(),
            actor_type: "user".to_owned(),
            actor_id: "user-1".to_owned(),
            dedupe_key: "continuity-main-transfer".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(transferred_project.owner_identity_id, new_identity);
    assert_eq!(transferred_main.owner_identity_id, new_identity);

    let old_after_transfer = attention
        .agent_detail("user-1", old_identity, 10)
        .await
        .unwrap();
    let new_after_transfer = attention
        .agent_detail("user-1", new_identity, 10)
        .await
        .unwrap();
    assert_eq!(old_after_transfer.open_commitment_count, 0);
    assert!(old_after_transfer.current_focus.is_none());
    assert_eq!(new_after_transfer.open_commitment_count, 2);
    assert_eq!(
        new_after_transfer
            .current_focus
            .as_ref()
            .map(|item| item.task_id.as_str()),
        Some(task.id.as_str())
    );

    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent::task_transition(
            "continuity-task-done",
            task.id.clone(),
            project_id,
            "in_progress",
            "done",
            None,
            "system:workflow",
            "continuity delivery accepted",
            false,
            now,
        ),
    )
    .await
    .unwrap();
    let first = CoordinationOutcomeConsumer::new(Arc::clone(&db), "continuity-consumer-1")
        .run_once(100)
        .await
        .unwrap();
    assert_eq!(first.reconciled_events, 1);

    let old_inbox = AgentInboxRepo::list_inbox_items(
        &*db,
        db::AgentInboxListQuery {
            recipient_identity_id: old_identity.to_owned(),
            status: None,
            scope_type: None,
            scope_id: None,
            limit: 10,
        },
    )
    .await
    .unwrap();
    let new_inbox = AgentInboxRepo::list_inbox_items(
        &*db,
        db::AgentInboxListQuery {
            recipient_identity_id: new_identity.to_owned(),
            status: None,
            scope_type: None,
            scope_id: None,
            limit: 10,
        },
    )
    .await
    .unwrap();
    assert!(old_inbox.is_empty());
    assert_eq!(new_inbox.len(), 2);

    // A different consumer instance replaying the same durable event must
    // preserve one evidence row per commitment and one outcome item per
    // commitment/scope for the new owner.
    CoordinationOutcomeConsumer::new(Arc::clone(&db), "continuity-consumer-2")
        .run_once(100)
        .await
        .unwrap();
    assert_eq!(
        AgentCommitmentRepo::list_commitment_evidence(&*db, &project_commitment.id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        AgentInboxRepo::list_inbox_items(
            &*db,
            db::AgentInboxListQuery {
                recipient_identity_id: new_identity.to_owned(),
                status: None,
                scope_type: None,
                scope_id: None,
                limit: 10,
            },
        )
        .await
        .unwrap()
        .len(),
        2
    );
}
