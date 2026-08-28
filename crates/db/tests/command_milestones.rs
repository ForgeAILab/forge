use db::{
    create_sqlite_pool, run_migrations, AppendProjectMilestoneRevisionCommand, CommandReceiptRepo,
    CreateCommandReceipt, CreateProject, CreateProjectMilestone, CreateProjectMilestoneCommand,
    CreateProjectMilestoneRevision, CreateProjectReadinessSnapshot,
    CreateProjectReadinessSnapshotCommand, CreateProjectRelease, CreateProjectReleaseCommand,
    DbError, DomainEventRepo, ProjectOrchestrationRepo, ProjectRepo,
    SetPrimaryProjectMilestoneCommand, SqliteDb, User, UserRepo,
};
use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

const PROJECT_ID: &str = "milestone-command-project";
const NOW: &str = "2026-08-20T00:00:00.000Z";

async fn project_source_watermark(db: &SqliteDb) -> String {
    sqlx::query_scalar(
        "SELECT COALESCE(
             (SELECT id FROM domain_event
              WHERE scope_type = 'project' AND scope_id = ?
                AND event_type != 'milestone.readiness.evaluated'
              ORDER BY sequence DESC LIMIT 1),
             'none'
         )",
    )
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("project source watermark")
}

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

async fn project(db: &SqliteDb) {
    UserRepo::create_user(
        db,
        &User {
            id: "user-1".to_owned(),
            email: "milestone-command@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Milestone command test".to_owned()),
            is_admin: false,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("user");
    ProjectRepo::create(
        db,
        CreateProject {
            id: PROJECT_ID.to_owned(),
            name: "Milestone command project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some("user-1".to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("project");
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
        scope_id: PROJECT_ID.to_owned(),
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
        committed_at: NOW.to_owned(),
    }
}

fn milestone_revision(
    id: &str,
    milestone_id: &str,
    expected_milestone_version: i64,
    base_revision: i64,
    base_revision_id: Option<&str>,
    lifecycle: &str,
) -> CreateProjectMilestoneRevision {
    CreateProjectMilestoneRevision {
        id: id.to_owned(),
        milestone_id: milestone_id.to_owned(),
        expected_milestone_version,
        base_revision,
        base_revision_id: base_revision_id.map(str::to_owned),
        lifecycle: lifecycle.to_owned(),
        display_label: Some("First milestone".to_owned()),
        outcome: "The outcome is delivered".to_owned(),
        included_scope_json: "[]".to_owned(),
        excluded_scope_json: "[]".to_owned(),
        charter_revision_id: None,
        document_revisions_json: "[]".to_owned(),
        task_selection_json: "[]".to_owned(),
        dependencies_json: "[]".to_owned(),
        risks_json: "[]".to_owned(),
        acceptance_checks_json: "[]".to_owned(),
        evidence_requirements_json: "[]".to_owned(),
        known_issues_json: "[]".to_owned(),
        change_summary: "command test".to_owned(),
        schema_version: "forge.milestone-definition/v1".to_owned(),
        render_version: "forge.milestone-definition-render/v1".to_owned(),
        rendered_view: "# First milestone".to_owned(),
        content_digest: format!("content-{id}"),
        rendered_digest: format!("rendered-{id}"),
        author_type: "user".to_owned(),
        author_id: Some("user-1".to_owned()),
        source_refs_json: "[]".to_owned(),
        created_at: NOW.to_owned(),
    }
}

#[tokio::test]
async fn milestone_commands_commit_event_receipt_and_replay_exactly() {
    let db = database().await;
    project(&db).await;

    let milestone_id = "milestone-1";
    let revision_id = "milestone-revision-1";
    let create_receipt = receipt(
        "project.milestone",
        "milestone-create-1",
        "digest-milestone-create-1",
        serde_json::json!({
            "project_id": PROJECT_ID,
            "milestone_id": milestone_id,
            "revision_id": revision_id,
        }),
    );
    let create = CreateProjectMilestoneCommand {
        milestone: CreateProjectMilestone {
            id: milestone_id.to_owned(),
            project_id: PROJECT_ID.to_owned(),
            expected_project_version: 1,
            milestone_sequence: 1,
            milestone_key: "M001".to_owned(),
            display_label: Some("First milestone".to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        revision: milestone_revision(revision_id, milestone_id, 1, 0, None, "draft"),
        allocate_project_sequence: false,
        check_definitions: Vec::new(),
        command_receipt: Some(create_receipt.clone()),
        action_execution: None,
    };
    let first = ProjectOrchestrationRepo::create_project_milestone_command(&db, create.clone())
        .await
        .expect("milestone create command");
    let replay = ProjectOrchestrationRepo::create_project_milestone_command(&db, create)
        .await
        .expect("milestone create replay");
    assert_eq!(first, replay);
    assert_eq!(first.revision, 1);

    let revision_2_id = "milestone-revision-2";
    let revise_receipt = receipt(
        "project.milestone",
        "milestone-revise-1",
        "digest-milestone-revise-1",
        serde_json::json!({
            "project_id": PROJECT_ID,
            "milestone_id": milestone_id,
            "revision_id": revision_2_id,
        }),
    );
    let revise = AppendProjectMilestoneRevisionCommand {
        revision: milestone_revision(revision_2_id, milestone_id, 2, 0, None, "proposed"),
        check_definitions: Vec::new(),
        command_receipt: Some(revise_receipt.clone()),
        action_execution: None,
    };
    let revised =
        ProjectOrchestrationRepo::append_project_milestone_revision_command(&db, revise.clone())
            .await
            .expect("milestone revise command");
    assert_eq!(revised.revision, 2);
    let revised_replay =
        ProjectOrchestrationRepo::append_project_milestone_revision_command(&db, revise)
            .await
            .expect("milestone revise replay");
    assert_eq!(revised, revised_replay);

    let primary_receipt = receipt(
        "project.milestone",
        "milestone-primary-1",
        "digest-milestone-primary-1",
        serde_json::json!({
            "project_id": PROJECT_ID,
            "primary_milestone_id": milestone_id,
        }),
    );
    let primary = SetPrimaryProjectMilestoneCommand {
        project_id: PROJECT_ID.to_owned(),
        primary_milestone_id: Some(milestone_id.to_owned()),
        expected_project_version: 2,
        principal_type: "user".to_owned(),
        principal_id: "user-1".to_owned(),
        authorization_basis: "test".to_owned(),
        authorization_action: "project.milestone.primary.set".to_owned(),
        authorization_occurred_at: NOW.to_owned(),
        explicit_event: "user-event-primary".to_owned(),
        idempotency_key: "milestone-primary-1".to_owned(),
        updated_at: NOW.to_owned(),
        command_receipt: Some(primary_receipt),
        action_execution: None,
    };
    let updated =
        ProjectOrchestrationRepo::set_primary_project_milestone_command(&db, primary.clone())
            .await
            .expect("primary command");
    assert_eq!(updated.primary_milestone_id.as_deref(), Some(milestone_id));
    let updated_replay =
        ProjectOrchestrationRepo::set_primary_project_milestone_command(&db, primary)
            .await
            .expect("primary replay");
    assert_eq!(updated, updated_replay);

    let receipt_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM command_receipt WHERE scope_id = ?")
            .bind(PROJECT_ID)
            .fetch_one(db.pool())
            .await
            .expect("receipt count");
    assert_eq!(receipt_count, 3);
    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE scope_type = 'project' AND scope_id = ?",
    )
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("event count");
    // Project creation records two project-scoped events before the command
    // events exercised above.
    assert_eq!(event_count, 5);

    let mut altered = create_receipt;
    altered.input_digest = "different-digest".to_owned();
    let conflict = ProjectOrchestrationRepo::create_project_milestone_command(
        &db,
        CreateProjectMilestoneCommand {
            milestone: CreateProjectMilestone {
                id: milestone_id.to_owned(),
                project_id: PROJECT_ID.to_owned(),
                expected_project_version: 1,
                milestone_sequence: 1,
                milestone_key: "M001".to_owned(),
                display_label: Some("First milestone".to_owned()),
                created_at: NOW.to_owned(),
                updated_at: NOW.to_owned(),
            },
            revision: milestone_revision(revision_id, milestone_id, 1, 0, None, "draft"),
            allocate_project_sequence: false,
            check_definitions: Vec::new(),
            command_receipt: Some(altered),
            action_execution: None,
        },
    )
    .await
    .expect_err("changed digest must conflict");
    assert!(matches!(conflict, DbError::IdempotencyConflict));

    let stored = CommandReceiptRepo::get_command_receipt(
        &db,
        "user",
        "user-1",
        "project",
        PROJECT_ID,
        "project.milestone",
        "milestone-primary-1",
        "digest-milestone-primary-1",
    )
    .await
    .expect("receipt lookup");
    let stored = stored.expect("primary receipt");
    let event = DomainEventRepo::get_event(&db, &stored.event_id)
        .await
        .expect("event lookup")
        .expect("event");
    assert_eq!(event.actor_type, "user");
    assert_eq!(event.actor_id.as_deref(), Some("user-1"));
    assert_eq!(event.correlation_id, stored.correlation_id);
    assert_eq!(event.causation_id, stored.causation_id);
    assert_eq!(event.causation_depth, stored.causation_depth);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_milestone_create_replay_returns_the_first_revision() {
    let (db, path) = file_database("milestone-create-race").await;
    project(&db).await;
    let first_receipt = receipt(
        "project.milestone",
        "milestone-create-race",
        "digest-milestone-create-race",
        serde_json::json!({
            "operation": "project.milestone",
            "project_id": PROJECT_ID,
            "milestone_id": "milestone-first",
            "revision_id": "milestone-revision-first",
        }),
    );
    let second_receipt = CreateCommandReceipt {
        id: "receipt-milestone-create-race-second".to_owned(),
        outcome_json: serde_json::json!({
            "operation": "project.milestone",
            "project_id": PROJECT_ID,
            "milestone_id": "milestone-second",
            "revision_id": "milestone-revision-second",
        })
        .to_string(),
        ..first_receipt.clone()
    };
    let first = CreateProjectMilestoneCommand {
        milestone: CreateProjectMilestone {
            id: "milestone-first".to_owned(),
            project_id: PROJECT_ID.to_owned(),
            expected_project_version: 1,
            milestone_sequence: 1,
            milestone_key: "M001".to_owned(),
            display_label: Some("First milestone".to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        revision: milestone_revision(
            "milestone-revision-first",
            "milestone-first",
            1,
            0,
            None,
            "draft",
        ),
        allocate_project_sequence: false,
        check_definitions: Vec::new(),
        command_receipt: Some(first_receipt),
        action_execution: None,
    };
    let second = CreateProjectMilestoneCommand {
        milestone: CreateProjectMilestone {
            id: "milestone-second".to_owned(),
            project_id: PROJECT_ID.to_owned(),
            expected_project_version: 1,
            milestone_sequence: 1,
            milestone_key: "M001".to_owned(),
            display_label: Some("First milestone".to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        revision: milestone_revision(
            "milestone-revision-second",
            "milestone-second",
            1,
            0,
            None,
            "draft",
        ),
        allocate_project_sequence: false,
        check_definitions: Vec::new(),
        command_receipt: Some(second_receipt),
        action_execution: None,
    };
    let (first, second) = tokio::join!(
        ProjectOrchestrationRepo::create_project_milestone_command(&db, first),
        ProjectOrchestrationRepo::create_project_milestone_command(&db, second),
    );
    let first = first.expect("first milestone command");
    let second = second.expect("concurrent milestone replay");
    assert_eq!(second, first);
    assert!(matches!(
        first.id.as_str(),
        "milestone-revision-first" | "milestone-revision-second"
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project_milestone")
            .fetch_one(db.pool())
            .await
            .expect("milestone count"),
        1
    );
    db.pool().close().await;
    let _ = std::fs::remove_file(path);
}

async fn baseline_fixture(db: &SqliteDb, milestone_id: &str, revision_id: &str) {
    sqlx::query(
        "INSERT INTO project_charter
            (id, account_id, genesis_session_id, project_id, current_draft_revision_id,
             current_approved_revision_id, project_mode, maturity, lifecycle, version,
             created_at, updated_at)
         VALUES ('charter-1', 'user-1', NULL, ?, NULL, NULL, 'compact', 'mvp',
                 'attached', 1, ?, ?)",
    )
    .bind(PROJECT_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("charter");
    sqlx::query(
        "INSERT INTO project_charter_revision
            (id, charter_id, revision, base_revision, base_revision_id, lifecycle,
            schema_version, render_version, content_json, rendered_view, change_summary,
            author_type, author_id, source_message_id,
             source_turn_job_id, source_refs_json, content_digest, rendered_digest, created_at)
         VALUES ('charter-revision-1', 'charter-1', 1, 0, NULL, 'approved',
                 'charter@1', 'render@1', '{}', '# Charter',
                 'fixture', 'user', 'user-1', NULL, NULL, '[]', 'charter-content',
                 'charter-rendered', ?)",
    )
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("charter revision");
    sqlx::query(
        "UPDATE project_charter
         SET current_approved_revision_id = 'charter-revision-1'
         WHERE id = 'charter-1'",
    )
    .execute(db.pool())
    .await
    .expect("charter pointer");
    sqlx::query(
        "INSERT INTO project_execution_baseline
            (id, project_id, current_revision_id, lifecycle, version, created_at, updated_at)
         VALUES ('baseline-1', ?, 'baseline-revision-1', 'active', 1, ?, ?)",
    )
    .bind(PROJECT_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("baseline");
    sqlx::query(
        "INSERT INTO project_execution_baseline_revision
            (id, baseline_id, revision, base_revision, base_revision_id, lifecycle,
             charter_revision_id, document_revisions_json, plan_items_json, milestone_id,
             milestone_ids_json, milestone_definition_revision_ids_json, primary_milestone_id,
             release_policy_json, release_policy_revision, release_policy_digest,
             acceptance_matrix_json, capability_classes_json, risk_classes_json,
             adaptive_envelope_json, elevated_operations_json, exclusions_json,
             rollback_recovery_json, schema_version, render_version, rendered_view,
             content_digest, rendered_digest, source_refs_json, created_at)
         VALUES ('baseline-revision-1', 'baseline-1', 1, 0, NULL, 'approved',
                 'charter-revision-1', '[]', '[]', ?, ?, ?, NULL, '{}',
                 'policy@1', 'policy-digest-1', '[]', '[]', '[]', '{}', '[]', '[]',
                 '{}', 'baseline@1', 'baseline-render@1', '# Baseline',
                 'baseline-digest-1', 'baseline-rendered-1', '[]', ?)",
    )
    .bind(milestone_id)
    .bind(format!("[\"{milestone_id}\"]"))
    .bind(format!("[\"{revision_id}\"]"))
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("baseline revision");
    sqlx::query(
        "UPDATE project_milestone
         SET lifecycle = 'active', current_definition_revision_id = ?, version = 1
         WHERE id = ?",
    )
    .bind(revision_id)
    .bind(milestone_id)
    .execute(db.pool())
    .await
    .expect("active milestone");
}

#[tokio::test]
async fn readiness_and_release_commands_are_atomic_and_replay_exactly() {
    let db = database().await;
    project(&db).await;
    sqlx::query(
        "INSERT INTO project_milestone
            (id, project_id, milestone_sequence, milestone_key, display_label, lifecycle,
             blocker_reason_json, stale_reason_json, reconciliation_reason_json, version,
             created_at, updated_at)
         VALUES ('milestone-2', ?, 1, 'M001', 'First milestone', 'active', '[]', '[]', '[]', 1, ?, ?)",
    )
    .bind(PROJECT_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("milestone");
    sqlx::query(
        "INSERT INTO project_milestone_revision
            (id, milestone_id, revision, base_revision, base_revision_id, lifecycle,
             display_label, outcome, included_scope_json, excluded_scope_json,
             charter_revision_id, document_revisions_json, task_selection_json,
             dependencies_json, risks_json, acceptance_checks_json,
             evidence_requirements_json, known_issues_json, change_summary,
             schema_version, render_version, rendered_view, content_digest,
             rendered_digest, author_type, author_id, source_refs_json, created_at)
         VALUES ('milestone-revision-2', 'milestone-2', 1, 0, NULL, 'approved',
                 'First milestone', 'The outcome is delivered', '[]', '[]', NULL, '[]',
                 '[]', '[]', '[]', '[]', '[]', '[]', 'fixture', 'milestone@1',
                 'milestone-render@1', '# Milestone', 'milestone-content-2',
                 'milestone-rendered-2', 'user', 'user-1', '[]', ?)",
    )
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("milestone revision");
    sqlx::query(
        "UPDATE project_milestone SET current_definition_revision_id = 'milestone-revision-2'
         WHERE id = 'milestone-2'",
    )
    .execute(db.pool())
    .await
    .expect("milestone pointer");
    baseline_fixture(&db, "milestone-2", "milestone-revision-2").await;
    let source_watermark = project_source_watermark(&db).await;

    let readiness_id = "readiness-1";
    let readiness_receipt = receipt(
        "project.milestone.readiness",
        "readiness-1",
        "digest-readiness-1",
        serde_json::json!({
            "project_id": PROJECT_ID,
            "milestone_id": "milestone-2",
            "readiness_snapshot_id": readiness_id,
        }),
    );
    let readiness = CreateProjectReadinessSnapshotCommand {
        snapshot: CreateProjectReadinessSnapshot {
            id: readiness_id.to_owned(),
            project_id: PROJECT_ID.to_owned(),
            milestone_id: "milestone-2".to_owned(),
            definition_revision_id: "milestone-revision-2".to_owned(),
            baseline_id: "baseline-1".to_owned(),
            baseline_revision_id: "baseline-revision-1".to_owned(),
            baseline_digest: "baseline-digest-1".to_owned(),
            release_policy_revision: "policy@1".to_owned(),
            release_policy_digest: "policy-digest-1".to_owned(),
            input_manifest_json: "[]".to_owned(),
            event_watermark: source_watermark.clone(),
            outcome: "ready".to_owned(),
            blocking_reasons_json: "[]".to_owned(),
            blocker_projection_json: "[]".to_owned(),
            stale_projection_json: "[]".to_owned(),
            reconciliation_projection_json: "[]".to_owned(),
            check_results_json: "[]".to_owned(),
            waiver_manifest_json: "[]".to_owned(),
            evidence_manifest_json: "[]".to_owned(),
            commit_context_json: "{}".to_owned(),
            computing_policy_revision: "forge.readiness.compute/v1".to_owned(),
            readiness_digest: "readiness-digest-1".to_owned(),
            principal_type: "user".to_owned(),
            principal_id: "user-1".to_owned(),
            authorization_basis: "test".to_owned(),
            authorization_action: "project.milestone.readiness".to_owned(),
            authorization_occurred_at: NOW.to_owned(),
            expected_milestone_version: 1,
            explicit_event: "readiness-event-1".to_owned(),
            idempotency_key: "readiness-1".to_owned(),
            created_at: NOW.to_owned(),
        },
        command_receipt: Some(readiness_receipt),
        action_execution: None,
    };
    let snapshot =
        ProjectOrchestrationRepo::create_project_readiness_snapshot_command(&db, readiness.clone())
            .await
            .expect("readiness command");
    let snapshot_replay =
        ProjectOrchestrationRepo::create_project_readiness_snapshot_command(&db, readiness.clone())
            .await
            .expect("readiness replay");
    assert_eq!(snapshot, snapshot_replay);

    // A source mutation after candidate computation must invalidate a new
    // persistence attempt, even when the milestone version is unchanged.
    DomainEventRepo::append_event(
        &db,
        db::CreateDomainEvent {
            id: "source-event-after-readiness".to_owned(),
            event_type: "project.document.revised".to_owned(),
            entity_type: "project_document_revision".to_owned(),
            entity_id: "document-revision-1".to_owned(),
            actor_type: "user".to_owned(),
            actor_id: Some("user-1".to_owned()),
            scope_type: "project".to_owned(),
            scope_id: PROJECT_ID.to_owned(),
            correlation_id: "source-mutation-correlation".to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("source-event-after-readiness".to_owned()),
            payload_json: "{}".to_owned(),
            created_at: NOW.to_owned(),
        },
    )
    .await
    .expect("source mutation event");
    let mut stale_readiness = readiness;
    stale_readiness.snapshot.id = "readiness-stale-source".to_owned();
    stale_readiness.snapshot.expected_milestone_version = 2;
    stale_readiness.snapshot.explicit_event = "readiness-event-stale-source".to_owned();
    stale_readiness.snapshot.idempotency_key = "readiness-stale-source".to_owned();
    stale_readiness.command_receipt = Some(receipt(
        "project.milestone.readiness",
        "readiness-stale-source",
        "digest-readiness-stale-source",
        serde_json::json!({
            "project_id": PROJECT_ID,
            "milestone_id": "milestone-2",
            "readiness_snapshot_id": "readiness-stale-source",
        }),
    ));
    let stale =
        ProjectOrchestrationRepo::create_project_readiness_snapshot_command(&db, stale_readiness)
            .await
            .expect_err("source mutation must invalidate readiness candidate");
    assert!(matches!(stale, DbError::VersionConflict));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_readiness_snapshot
             WHERE id = 'readiness-stale-source'",
        )
        .fetch_one(db.pool())
        .await
        .expect("stale snapshot count"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE id = 'receipt-readiness-stale-source'",
        )
        .fetch_one(db.pool())
        .await
        .expect("stale receipt count"),
        0
    );

    let release_id = "release-1";
    let release_receipt = receipt(
        "project.release",
        "release-1",
        "digest-release-1",
        serde_json::json!({
            "project_id": PROJECT_ID,
            "milestone_id": "milestone-2",
            "release_id": release_id,
        }),
    );
    let release = CreateProjectReleaseCommand {
        release: CreateProjectRelease {
            id: release_id.to_owned(),
            project_id: PROJECT_ID.to_owned(),
            milestone_id: "milestone-2".to_owned(),
            expected_milestone_version: 2,
            release_sequence: 1,
            release_revision: 1,
            release_identifier: "M001-r1".to_owned(),
            milestone_revision_id: "milestone-revision-2".to_owned(),
            readiness_snapshot_id: readiness_id.to_owned(),
            readiness_digest: "readiness-digest-1".to_owned(),
            baseline_id: "baseline-1".to_owned(),
            baseline_revision_id: "baseline-revision-1".to_owned(),
            baseline_digest: "baseline-digest-1".to_owned(),
            release_policy_revision: "policy@1".to_owned(),
            release_policy_digest: "policy-digest-1".to_owned(),
            summary: "Released".to_owned(),
            changelog: "Initial release".to_owned(),
            known_issues_json: "[]".to_owned(),
            charter_revision_id: None,
            document_revisions_json: "[]".to_owned(),
            decision_ids_json: "[]".to_owned(),
            task_references_json: "[]".to_owned(),
            validation_references_json: "[]".to_owned(),
            git_references_json: "[]".to_owned(),
            evidence_references_json: "[]".to_owned(),
            waivers_json: "[]".to_owned(),
            releasing_principal_type: "user".to_owned(),
            releasing_principal_id: "user-1".to_owned(),
            authorization_basis: "test".to_owned(),
            authorization_action: "project.milestone.release".to_owned(),
            authorization_occurred_at: NOW.to_owned(),
            explicit_event: "release-event-1".to_owned(),
            schema_version: "forge.milestone-release/v1".to_owned(),
            snapshot_digest: "snapshot-digest-1".to_owned(),
            idempotency_key: "release-1".to_owned(),
            created_at: NOW.to_owned(),
        },
        references: Vec::new(),
        command_receipt: Some(release_receipt),
        action_execution: None,
    };
    let created = ProjectOrchestrationRepo::create_project_release_command(&db, release.clone())
        .await
        .expect("release command");
    let created_replay = ProjectOrchestrationRepo::create_project_release_command(&db, release)
        .await
        .expect("release replay");
    assert_eq!(created, created_replay);
    assert_eq!(created.release_identifier, "M001-r1");
    let lifecycle: String =
        sqlx::query_scalar("SELECT lifecycle FROM project_milestone WHERE id = 'milestone-2'")
            .fetch_one(db.pool())
            .await
            .expect("released lifecycle");
    assert_eq!(lifecycle, "released");
}

/// An agent-observed acceptance result is receipt-backed and replays exactly.
///
/// `task_validation` exists because an acceptance check asserts integrated
/// behaviour, which is wider than the one Task under review: a check can cover
/// a feature delivered earlier that later work has to keep working. Recording
/// it is therefore its own authorized command rather than a by-product of a
/// single Task's review, and like every other Project command it commits the
/// row, its domain event, and its receipt in one transaction.
#[tokio::test]
async fn agent_validation_results_commit_with_a_receipt_and_replay_exactly() {
    let db = database().await;
    project(&db).await;

    let milestone_id = "milestone-validation";
    let revision_id = "milestone-validation-revision";
    let check_id = "check-integrated-behaviour";
    let create = CreateProjectMilestoneCommand {
        milestone: CreateProjectMilestone {
            id: milestone_id.to_owned(),
            project_id: PROJECT_ID.to_owned(),
            expected_project_version: 1,
            milestone_sequence: 1,
            milestone_key: "M001".to_owned(),
            display_label: Some("Validation milestone".to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        revision: CreateProjectMilestoneRevision {
            acceptance_checks_json: serde_json::json!([{
                "id": check_id,
                "description": "Feature A still works alongside feature B",
                "required": true,
                "source_kind": "task_validation",
                "expected_result": "passed",
            }])
            .to_string(),
            ..milestone_revision(revision_id, milestone_id, 1, 0, None, "approved")
        },
        allocate_project_sequence: false,
        check_definitions: vec![db::CreateProjectMilestoneCheck {
            id: check_id.to_owned(),
            project_id: PROJECT_ID.to_owned(),
            milestone_id: milestone_id.to_owned(),
            definition_revision_id: revision_id.to_owned(),
            expected_milestone_version: 1,
            check_key: check_id.to_owned(),
            description: "Feature A still works alongside feature B".to_owned(),
            required: true,
            source_kind: "task_validation".to_owned(),
            expected_result: "passed".to_owned(),
            evidence_required: false,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        }],
        command_receipt: Some(receipt(
            "project.milestone",
            "validation-milestone-create",
            "digest-validation-milestone-create",
            serde_json::json!({
                "project_id": PROJECT_ID,
                "milestone_id": milestone_id,
                "revision_id": revision_id,
            }),
        )),
        action_execution: None,
    };
    ProjectOrchestrationRepo::create_project_milestone_command(&db, create)
        .await
        .expect("milestone with an agent-verifiable check");

    let result_id = "validation-result-1";
    let command = db::AppendProjectMilestoneCheckResultCommand {
        result: db::CreateProjectMilestoneCheckResult {
            id: result_id.to_owned(),
            project_id: PROJECT_ID.to_owned(),
            milestone_id: milestone_id.to_owned(),
            check_id: check_id.to_owned(),
            definition_revision_id: revision_id.to_owned(),
            outcome: "passed".to_owned(),
            source_kind: "task_validation".to_owned(),
            source_manifest_json: serde_json::json!({
                "result": "Exercised feature A and feature B together after B landed.",
                "governing_revision_ids": ["charter-revision-1", "baseline-revision-1"],
            })
            .to_string(),
            input_digest: "digest-validation-observation".to_owned(),
            // This test pins the command/receipt/replay contract; the
            // governing revision columns have their own cross-Project triggers
            // and are exercised by the readiness suite.
            governing_charter_revision_id: None,
            governing_baseline_revision_id: None,
            principal_type: "agent".to_owned(),
            principal_id: "project-agent-1".to_owned(),
            authorization_basis: "project_agent_binding_policy".to_owned(),
            authorization_action: "project.validation.record".to_owned(),
            authorization_occurred_at: NOW.to_owned(),
            expected_version: 1,
            explicit_event: "agent-action:validation-1".to_owned(),
            idempotency_key: "validation-record-1".to_owned(),
            created_at: NOW.to_owned(),
        },
        command_receipt: Some(receipt(
            "project.validation",
            "validation-record-1",
            "digest-validation-record-1",
            serde_json::json!({
                "project_id": PROJECT_ID,
                "milestone_id": milestone_id,
                "check_id": check_id,
                "result_id": result_id,
            }),
        )),
        action_execution: None,
    };

    let recorded =
        ProjectOrchestrationRepo::append_project_milestone_check_result(&db, command.clone())
            .await
            .expect("agent validation result commits");
    assert_eq!(recorded.id, result_id);
    assert_eq!(recorded.source_kind, "task_validation");
    assert_eq!(recorded.principal_type, "agent");

    let replay = ProjectOrchestrationRepo::append_project_milestone_check_result(&db, command)
        .await
        .expect("agent validation result replays");
    assert_eq!(
        recorded, replay,
        "a response-loss retry replays the receipt"
    );

    let receipts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM command_receipt WHERE operation = 'project.validation'",
    )
    .fetch_one(db.pool())
    .await
    .expect("receipt count");
    assert_eq!(receipts, 1, "the replay must not mint a second receipt");

    let events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'project.milestone.check.recorded'",
    )
    .fetch_one(db.pool())
    .await
    .expect("event count");
    assert_eq!(events, 1, "the result is announced exactly once");
}
