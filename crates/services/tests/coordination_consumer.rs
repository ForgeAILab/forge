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
async fn runtime_upgrade_preserves_cursor_ignores_legacy_lease_and_reconciles_once() {
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
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "Deliver outcome".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "shipped".to_owned(),
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

    let mut effective_workflow = services::workflow::default_workflow::default_workflow();
    effective_workflow
        .states
        .iter_mut()
        .find(|state| state.name == "done")
        .unwrap()
        .name = "shipped".to_owned();
    let snapshot = services::workflow::transition_event::transition_workflow_snapshot(
        &task,
        &effective_workflow,
        "in_progress",
        "shipped",
    )
    .unwrap();
    let old = db
        .append_event(CreateDomainEvent::task_transition(
            "old-task-transition",
            task.id.clone(),
            project_id,
            "in_progress",
            "shipped",
            None,
            "system:workflow",
            "old delivered event",
            false,
            now.clone(),
            snapshot.clone(),
        ))
        .await
        .unwrap();
    sqlx::query("INSERT INTO event_consumer_cursor (consumer_name, last_sequence, updated_at) VALUES (?, ?, ?)")
        .bind(coordination_consumer_name()).bind(old.sequence).bind(&now).execute(db.pool()).await.unwrap();
    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent::task_transition(
            "task-transition-outcome",
            task.id.clone(),
            project_id,
            "in_progress",
            "shipped",
            None,
            "system:workflow",
            "delivery accepted",
            false,
            now,
            snapshot,
        ),
    )
    .await
    .unwrap();

    // Replay must use the committed custom terminal meaning, even when the
    // current definition would interpret the same state as ongoing work.
    effective_workflow
        .states
        .iter_mut()
        .find(|state| state.name == "shipped")
        .unwrap()
        .kind = api_types::StateKind::Active;
    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(serde_json::to_string(&effective_workflow).unwrap())
        .bind(project_id)
        .execute(db.pool())
        .await
        .unwrap();

    let wanted = db
        .get_event("task-transition-outcome")
        .await
        .unwrap()
        .unwrap();
    sqlx::raw_sql(include_str!("../../db/tests/fixtures/event_delivery.sql"))
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO event_processing_lease (consumer_name, event_sequence, lease_owner, leased_until, attempts, updated_at) VALUES (?, ?, 'legacy', '2999-01-01T00:00:00Z', 1, ?)")
        .bind(coordination_consumer_name()).bind(wanted.sequence).bind(now_rfc3339()).execute(db.pool()).await.unwrap();
    sqlx::raw_sql(include_str!(
        "../../db/migrations/V202610020700__retire_event_delivery_leases.sql"
    ))
    .execute(db.pool())
    .await
    .unwrap();
    let preparing = CoordinationOutcomeConsumer::new(Arc::clone(&db));
    let services::worker_runtime::Outcome::Done(prepared) =
        services::worker_runtime::Worker::handle(&preparing, &wanted)
            .await
            .unwrap()
    else {
        panic!("wanted outcome");
    };
    sqlx::query("UPDATE agent_commitment SET version = version + 1 WHERE id = ?")
        .bind(&commitment.id)
        .execute(db.pool())
        .await
        .unwrap();
    let mut stale_tx = db::begin_immediate(db.pool()).await.unwrap();
    let stale =
        services::worker_runtime::Worker::commit(&preparing, &mut stale_tx, &wanted, &prepared)
            .await
            .unwrap_err();
    assert_eq!(
        stale.kind,
        services::worker_runtime::WorkerErrorKind::Transient,
        "preparation must retain the commitment CAS version"
    );
    stale_tx.rollback().await.unwrap();
    sqlx::raw_sql("CREATE TRIGGER fail_outcome_after_commitment BEFORE INSERT ON agent_inbox_item WHEN NEW.kind = 'task_outcome' BEGIN SELECT RAISE(ABORT, 'test rollback after commitment write'); END;")
        .execute(db.pool()).await.unwrap();
    let failed = CoordinationOutcomeConsumer::new(Arc::clone(&db))
        .run_once(100)
        .await
        .unwrap();
    assert_eq!(failed.processed_events, 0);
    assert_eq!(
        db.get_consumer_cursor(coordination_consumer_name())
            .await
            .unwrap()
            .unwrap()
            .last_sequence,
        old.sequence
    );
    assert_eq!(
        db.get_commitment(&commitment.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        db::AgentCommitmentStatus::InProgress
    );
    assert!(db
        .list_commitment_evidence(&commitment.id)
        .await
        .unwrap()
        .is_empty());
    sqlx::query("DROP TRIGGER fail_outcome_after_commitment")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE worker_health SET retry_not_before = '2000-01-01T00:00:00Z'")
        .execute(db.pool())
        .await
        .unwrap();
    let first = CoordinationOutcomeConsumer::new(Arc::clone(&db))
        .run_once(100)
        .await
        .unwrap();
    assert_eq!(first.claimed_events, 1);
    assert_eq!(first.last_sequence, wanted.sequence);
    assert_eq!(
        CoordinationOutcomeConsumer::new(Arc::clone(&db))
            .run_once(100)
            .await
            .unwrap()
            .processed_events,
        0
    );
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
    // cursor checkpoint.  A new process/lease owner must replay the event.
    sqlx::query(
        "UPDATE event_consumer_cursor SET last_sequence = ?, version = version + 1
         WHERE consumer_name = ?",
    )
    .bind(old.sequence)
    .bind(coordination_consumer_name())
    .execute(db.pool())
    .await
    .unwrap();

    let replay = CoordinationOutcomeConsumer::new(Arc::clone(&db))
        .run_once(100)
        .await
        .unwrap();
    assert!(replay.claimed_events >= 1);
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
            services::workflow::transition_event::transition_workflow_snapshot(
                &task,
                &services::workflow::default_workflow::default_workflow(),
                "in_progress",
                "done",
            )
            .unwrap(),
        ),
    )
    .await
    .unwrap();
    CoordinationOutcomeConsumer::new(Arc::clone(&db))
        .run_once(100)
        .await
        .unwrap();

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
    CoordinationOutcomeConsumer::new(Arc::clone(&db))
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

async fn audit_outcome_failure_fixture(
    status: db::AgentCommitmentStatus,
    replay_conflict: bool,
    concurrent: bool,
) {
    let database_path = format!(
        "/Volumes/Data/tmp/forge-coordination-race-{}.sqlite",
        new_uuid_v4()
    );
    let db = if concurrent {
        let pool = create_sqlite_pool(&format!("sqlite://{database_path}"))
            .await
            .unwrap();
        run_migrations(&pool).await.unwrap();
        Arc::new(SqliteDb::new(pool))
    } else {
        database().await
    };
    let identity_id = "identity-wedge";
    seed_identity(&db, identity_id).await;
    let now = now_rfc3339();
    let project_id = "project-wedge";
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.to_owned(),
            name: "Wedge Project".to_owned(),
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
    let mut tasks = Vec::new();
    for id in ["task-wedge", "task-behind"] {
        tasks.push(
            TaskRepo::create(
                &*db,
                CreateTask {
                    id: id.to_owned(),
                    project_id: project_id.to_owned(),
                    parent_task_id: None,
                    assignee_type: None,
                    assignee_id: None,
                    title: id.to_owned(),
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
            .unwrap(),
        );
    }
    let commitments = CommitmentService::new(Arc::clone(&db));
    let mut created = Vec::new();
    for task in &tasks {
        created.push(
            commitments
                .create(CreateCommitmentInput {
                    id: Some(format!("commitment-{}", task.id)),
                    owner_identity_id: identity_id.to_owned(),
                    scope_type: "project".to_owned(),
                    scope_id: project_id.to_owned(),
                    title: "Deliver".to_owned(),
                    description: None,
                    status: db::AgentCommitmentStatus::InProgress,
                    due_at: None,
                    correlation_id: format!("correlation-{}", task.id),
                    originating_action_id: None,
                    originating_task_id: Some(task.id.clone()),
                    evidence_required: true,
                })
                .await
                .unwrap(),
        );
    }
    if status == db::AgentCommitmentStatus::Cancelled {
        commitments
            .cancel(
                created[0].id.clone(),
                created[0].version,
                "no longer needed".into(),
                "user".into(),
                "user-1".into(),
                "cancel-wedge".into(),
            )
            .await
            .unwrap();
    } else if status == db::AgentCommitmentStatus::Blocked {
        sqlx::query(
            "UPDATE agent_commitment SET status = 'blocked', version = version + 1 WHERE id = ?",
        )
        .bind(&created[0].id)
        .execute(db.pool())
        .await
        .unwrap();
    }
    // ...and its Task still reaches done afterwards. Then another Task finishes.
    let workflow = services::workflow::default_workflow::default_workflow();
    for task in &tasks {
        let snapshot = services::workflow::transition_event::transition_workflow_snapshot(
            task,
            &workflow,
            "in_progress",
            "done",
        )
        .unwrap();
        DomainEventRepo::append_event(
            &*db,
            CreateDomainEvent::task_transition(
                format!("transition-{}", task.id),
                task.id.clone(),
                project_id,
                "in_progress",
                "done",
                None,
                "system:workflow",
                "delivered",
                false,
                now_rfc3339(),
                snapshot,
            ),
        )
        .await
        .unwrap();
    }
    let consumer = CoordinationOutcomeConsumer::new(Arc::clone(&db));
    if replay_conflict {
        consumer.run_once(1).await.unwrap();
        sqlx::query("UPDATE task SET version = version + 1 WHERE id = ?")
            .bind(&tasks[0].id)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE event_consumer_cursor SET last_sequence = 0 WHERE consumer_name = ?")
            .bind(coordination_consumer_name())
            .execute(db.pool())
            .await
            .unwrap();
    }
    if concurrent {
        let other = CoordinationOutcomeConsumer::new(Arc::clone(&db));
        let (a, b) = tokio::join!(consumer.run_once(100), other.run_once(100));
        assert!(a.is_ok() || b.is_ok());
    }
    consumer.run_once(100).await.unwrap();
    let cursor = db
        .get_consumer_cursor(coordination_consumer_name())
        .await
        .unwrap()
        .unwrap()
        .last_sequence;
    let (attempts, runtime_error, item_error): (i64, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT retry_attempts, runtime_error, item_error FROM worker_health WHERE worker_name = ?",
    )
    .bind(coordination_consumer_name())
    .fetch_one(db.pool())
    .await
    .unwrap();
    let dead: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM worker_dead_letter")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let behind = db
        .get_commitment(&created[1].id)
        .await
        .unwrap()
        .unwrap()
        .status;
    let last = db
        .get_event("transition-task-behind")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor, last.sequence);
    assert_eq!(attempts, 0);
    assert!(runtime_error.is_none() && item_error.is_none());
    assert_eq!(
        dead,
        i64::from(status == db::AgentCommitmentStatus::Blocked || replay_conflict)
    );
    assert_eq!(behind, db::AgentCommitmentStatus::Completed);
    if status == db::AgentCommitmentStatus::Cancelled {
        assert_eq!(
            db.get_commitment(&created[0].id)
                .await
                .unwrap()
                .unwrap()
                .status,
            db::AgentCommitmentStatus::Cancelled
        );
    }
}
#[tokio::test]
async fn audit_cancelled_commitment_is_noop_and_later_outcome_reconciles() {
    audit_outcome_failure_fixture(db::AgentCommitmentStatus::Cancelled, false, false).await;
}
#[tokio::test]
async fn audit_blocked_completion_is_terminal_and_later_outcome_reconciles() {
    audit_outcome_failure_fixture(db::AgentCommitmentStatus::Blocked, false, false).await;
}
#[tokio::test]
async fn audit_inbox_replay_dedupe_check_is_terminal() {
    audit_outcome_failure_fixture(db::AgentCommitmentStatus::InProgress, true, false).await;
}

#[tokio::test]
async fn audit_two_runtimes_race_coordination_outcomes_on_file_database() {
    audit_outcome_failure_fixture(db::AgentCommitmentStatus::InProgress, false, true).await;
}

/// Second-round audit: one commitment that rejects completion (blocked)
/// quarantines the whole outcome event. What happens to the *other*
/// commitment on the same Task and to the outcome inbox items?
#[tokio::test]
async fn audit2_blocked_commitment_failure_preserves_sibling_commitment_and_inbox() {
    let db = database().await;
    seed_identity(&db, "identity-a").await;
    sqlx::query("UPDATE agent_identity SET name = 'owner-a' WHERE id = 'identity-a'")
        .execute(db.pool())
        .await
        .unwrap();
    seed_identity(&db, "identity-b").await;
    let now = now_rfc3339();
    let project_id = "project-q";
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.to_owned(),
            name: "Quarantine Project".to_owned(),
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
            id: "task-q".to_owned(),
            project_id: project_id.to_owned(),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "task-q".to_owned(),
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
    let commitments = CommitmentService::new(Arc::clone(&db));
    let mut created = Vec::new();
    for owner in ["identity-a", "identity-b"] {
        created.push(
            commitments
                .create(CreateCommitmentInput {
                    id: Some(format!("commitment-{owner}")),
                    owner_identity_id: owner.to_owned(),
                    scope_type: "project".to_owned(),
                    scope_id: project_id.to_owned(),
                    title: "Deliver".to_owned(),
                    description: None,
                    status: db::AgentCommitmentStatus::InProgress,
                    due_at: None,
                    correlation_id: format!("correlation-{owner}"),
                    originating_action_id: None,
                    originating_task_id: Some(task.id.clone()),
                    evidence_required: true,
                })
                .await
                .unwrap(),
        );
    }
    // Owner A's commitment is blocked (as the consumer itself does on a
    // cancelled/blocked Task outcome, or the Agent does by hand).
    sqlx::query(
        "UPDATE agent_commitment SET status = 'blocked', version = version + 1 WHERE id = ?",
    )
    .bind(&created[0].id)
    .execute(db.pool())
    .await
    .unwrap();
    let workflow = services::workflow::default_workflow::default_workflow();
    let snapshot = services::workflow::transition_event::transition_workflow_snapshot(
        &task,
        &workflow,
        "in_progress",
        "done",
    )
    .unwrap();
    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent::task_transition(
            "transition-task-q".to_owned(),
            task.id.clone(),
            project_id,
            "in_progress",
            "done",
            None,
            "system:workflow",
            "delivered",
            false,
            now_rfc3339(),
            snapshot,
        ),
    )
    .await
    .unwrap();
    let consumer = CoordinationOutcomeConsumer::new(Arc::clone(&db));
    consumer.run_once(100).await.unwrap();
    consumer.run_once(100).await.unwrap();

    let dead: Vec<(String, String, String)> =
        sqlx::query_as("SELECT source_key, error_kind, last_error FROM worker_dead_letter")
            .fetch_all(db.pool())
            .await
            .unwrap();
    let a = db
        .get_commitment(&created[0].id)
        .await
        .unwrap()
        .unwrap()
        .status;
    let b = db
        .get_commitment(&created[1].id)
        .await
        .unwrap()
        .unwrap()
        .status;
    let outcome_items: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_inbox_item WHERE source_type = 'task_outcome' AND source_id = ?",
    )
    .bind(&task.id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    let cursor = db
        .get_consumer_cursor(coordination_consumer_name())
        .await
        .unwrap()
        .unwrap()
        .last_sequence;
    println!(
        "AUDIT2 dead={dead:?} a={a:?} b={b:?} outcome_inbox_items={outcome_items} cursor={cursor}"
    );
    assert_eq!(dead.len(), 1, "only the rejected commitment is quarantined");
    assert_eq!(a, db::AgentCommitmentStatus::Blocked);
    assert!(dead[0].0.contains(&created[0].id));
    assert!(dead[0].2.contains(&created[0].id));
    assert_eq!(b, db::AgentCommitmentStatus::Completed);
    assert_eq!(outcome_items, 2);
    let event = db.get_event("transition-task-q").await.unwrap().unwrap();
    assert_eq!(cursor, event.sequence);
    let operator = services::OperatorStatusService::new_for_test(Arc::clone(&db));
    operator.set_runtime_workers(&[services::RuntimeWorker::Coordination]);
    let status = operator.compute_status().await.unwrap();
    assert_eq!(status.event_consumers[0].dead_letter_count, 1);
    assert!(status.event_consumers[0].recent_dead_letters[0]
        .item_key
        .contains(&created[0].id));
}
