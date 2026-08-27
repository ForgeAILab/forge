//! Baseline-activation governance guarantees at the repository layer:
//! server-minted baseline shell ids, preplanned-Task governance backfill,
//! and the Project primary-milestone pointer.

use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, validate_uuid_v4,
    ActivateProjectExecutionBaseline, ApproveProjectExecutionBaseline, CreateProject,
    CreateProjectExecutionBaseline, CreateProjectExecutionBaselineRevision, CreateRepo, CreateTask,
    ProjectOrchestrationRepo, ProjectRepo, RepoRepo, SqliteDb, TaskRepo, WorkMode,
};
use sqlx::Row;

struct Fixture {
    db: SqliteDb,
    project_id: String,
    repo_id: String,
    charter_revision_id: String,
    milestone_id: String,
    milestone_definition_revision_id: String,
    now: String,
}

/// A Charter-backed Project with one repository binding and one planned
/// milestone, wired the same way the Charter-approval flow leaves it.
async fn charter_backed_project() -> Fixture {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    let db = SqliteDb::new(pool);
    let now = now_rfc3339();

    let user_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, created_at, updated_at)
         VALUES (?, ?, 'test', ?, ?)",
    )
    .bind(&user_id)
    .bind(format!("{user_id}@example.test"))
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("user fixture");

    let project_id = new_uuid_v4();
    ProjectRepo::create(
        &db,
        CreateProject {
            id: project_id.clone(),
            name: "Baseline Activation".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(user_id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project fixture");
    let repo_id = new_uuid_v4();
    RepoRepo::create(
        &db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "forge".to_owned(),
            remote_url: "https://example.com/forge.git".to_owned(),
            local_path: Some("/tmp/forge-baseline-activation-repo".to_owned()),
            work_mode: WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo fixture");

    let charter_id = new_uuid_v4();
    let charter_revision_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO project_charter
         (id, account_id, project_id, project_mode, maturity, lifecycle, created_at, updated_at)
         VALUES (?, ?, ?, 'standard', 'mvp', 'attached', ?, ?)",
    )
    .bind(&charter_id)
    .bind(&user_id)
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("charter fixture");
    sqlx::query(
        "INSERT INTO project_charter_revision
         (id, charter_id, revision, lifecycle, schema_version, render_version,
          content_json, rendered_view, author_type, author_id, content_digest,
          rendered_digest, created_at)
         VALUES (?, ?, 1, 'approved', 'test', 'test', '{}', 'charter',
                 'user', ?, 'content', 'rendered', ?)",
    )
    .bind(&charter_revision_id)
    .bind(&charter_id)
    .bind(&user_id)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("charter revision fixture");
    sqlx::query("UPDATE project_charter SET current_approved_revision_id = ? WHERE id = ?")
        .bind(&charter_revision_id)
        .bind(&charter_id)
        .execute(db.pool())
        .await
        .expect("charter pointer");
    sqlx::query(
        "UPDATE project
         SET charter_status = 'charter_backed', charter_setup_required = 0,
             current_charter_id = ?, current_charter_revision_id = ?,
             current_charter_version = 1
         WHERE id = ?",
    )
    .bind(&charter_id)
    .bind(&charter_revision_id)
    .bind(&project_id)
    .execute(db.pool())
    .await
    .expect("Project Charter binding");

    let milestone_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO project_milestone (
            id, project_id, milestone_sequence, milestone_key, display_label,
            lifecycle, blocker_reason_json, stale_reason_json,
            reconciliation_reason_json, version, created_at, updated_at
         ) VALUES (?, ?, 1, 'M001', 'First milestone', 'planned', '[]', '[]', '[]', 1, ?, ?)",
    )
    .bind(&milestone_id)
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("milestone fixture");
    let milestone_definition_revision_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO project_milestone_revision (
            id, milestone_id, revision, base_revision, base_revision_id, lifecycle,
            display_label, outcome, included_scope_json, excluded_scope_json,
            charter_revision_id, document_revisions_json, task_selection_json,
            dependencies_json, risks_json, acceptance_checks_json,
            evidence_requirements_json, known_issues_json, change_summary,
            schema_version, render_version, rendered_view, content_digest,
            rendered_digest, author_type, author_id, source_refs_json, created_at
         ) VALUES (?, ?, 1, 0, NULL, 'approved', 'First milestone',
                   'The delivered outcome is usable.', '[]', '[]', ?, '[]', '[]',
                   '[]', '[]', '[]', '[]', '[]', 'Initial definition', 'test',
                   'test', '# Milestone', 'milestone-content', 'milestone-rendered',
                   'user', NULL, '[]', ?)",
    )
    .bind(&milestone_definition_revision_id)
    .bind(&milestone_id)
    .bind(&charter_revision_id)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("milestone definition revision fixture");
    sqlx::query("UPDATE project_milestone SET current_definition_revision_id = ? WHERE id = ?")
        .bind(&milestone_definition_revision_id)
        .bind(&milestone_id)
        .execute(db.pool())
        .await
        .expect("milestone definition pointer");

    Fixture {
        db,
        project_id,
        repo_id,
        charter_revision_id,
        milestone_id,
        milestone_definition_revision_id,
        now,
    }
}

async fn seed_task(fixture: &Fixture, title: &str, status: &str) -> String {
    let task_id = new_uuid_v4();
    TaskRepo::create(
        &fixture.db,
        CreateTask {
            id: task_id.clone(),
            project_id: fixture.project_id.clone(),
            repo_id: Some(fixture.repo_id.clone()),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: title.to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: status.to_owned(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: fixture.now.clone(),
            updated_at: fixture.now.clone(),
        },
    )
    .await
    .expect("task fixture");
    task_id
}

async fn seed_prebaseline_governance(fixture: &Fixture, task_id: &str, plan_item_id: &str) {
    sqlx::query(
        "INSERT INTO project_task_governance
         (task_id, project_id, charter_revision_id, plan_item_id,
          document_revisions_json, capability_class, risk_class, runnable,
          provenance_json, version, created_at, updated_at)
         VALUES (?, ?, ?, ?, '[]', 'repository_write', 'low', 0,
                 '{\"baseline_pending\":true,\"schema\":\"forge.task-governance/v1\"}',
                 1, ?, ?)",
    )
    .bind(task_id)
    .bind(&fixture.project_id)
    .bind(&fixture.charter_revision_id)
    .bind(plan_item_id)
    .bind(&fixture.now)
    .bind(&fixture.now)
    .execute(fixture.db.pool())
    .await
    .expect("prebaseline governance fixture");
}

/// Propose, revise, approve, and activate a baseline through the repository
/// contract, covering `plan-item-1`/`plan-item-2` and the fixture milestone
/// as its primary.
async fn activate_baseline(fixture: &Fixture) {
    let baseline = ProjectOrchestrationRepo::create_project_execution_baseline(
        &fixture.db,
        CreateProjectExecutionBaseline {
            project_id: fixture.project_id.clone(),
            created_at: fixture.now.clone(),
            updated_at: fixture.now.clone(),
        },
    )
    .await
    .expect("baseline shell");
    let revision = ProjectOrchestrationRepo::create_project_execution_baseline_revision(
        &fixture.db,
        CreateProjectExecutionBaselineRevision {
            id: new_uuid_v4(),
            baseline_id: baseline.id.clone(),
            expected_baseline_version: baseline.version,
            base_revision: 0,
            base_revision_id: None,
            lifecycle: "proposed".to_owned(),
            charter_revision_id: fixture.charter_revision_id.clone(),
            document_revisions_json: "[]".to_owned(),
            plan_items_json: r#"[{"id":"plan-item-1"},{"id":"plan-item-2"}]"#.to_owned(),
            milestone_id: Some(fixture.milestone_id.clone()),
            milestone_ids_json: format!("[\"{}\"]", fixture.milestone_id),
            milestone_definition_revision_ids_json: format!(
                "[\"{}\"]",
                fixture.milestone_definition_revision_id
            ),
            primary_milestone_id: Some(fixture.milestone_id.clone()),
            release_policy_json: "{}".to_owned(),
            release_policy_revision: "release-policy@1".to_owned(),
            release_policy_digest: "release-policy-digest".to_owned(),
            acceptance_matrix_json: "[]".to_owned(),
            capability_classes_json: "[]".to_owned(),
            risk_classes_json: "[]".to_owned(),
            adaptive_envelope_json: "{}".to_owned(),
            elevated_operations_json: "[]".to_owned(),
            exclusions_json: "[]".to_owned(),
            rollback_recovery_json: "{}".to_owned(),
            schema_version: "forge.project-orchestration/v1".to_owned(),
            render_version: "1".to_owned(),
            rendered_view: "# Baseline".to_owned(),
            content_digest: "baseline-content-digest".to_owned(),
            rendered_digest: "baseline-rendered-digest".to_owned(),
            source_refs_json: "[]".to_owned(),
            created_at: fixture.now.clone(),
        },
    )
    .await
    .expect("baseline revision");
    let project_version: i64 = sqlx::query_scalar("SELECT version FROM project WHERE id = ?")
        .bind(&fixture.project_id)
        .fetch_one(fixture.db.pool())
        .await
        .expect("project version");
    let approval = ProjectOrchestrationRepo::approve_project_execution_baseline(
        &fixture.db,
        ApproveProjectExecutionBaseline {
            id: new_uuid_v4(),
            baseline_id: baseline.id.clone(),
            revision_id: revision.id.clone(),
            expected_baseline_version: baseline.version + 1,
            expected_project_version: project_version,
            principal_type: "user".to_owned(),
            principal_id: fixture.project_id.clone(),
            authorization_basis: "explicit baseline approval".to_owned(),
            authorization_action: "project.execution_baseline.approve".to_owned(),
            explicit_event: "approve baseline".to_owned(),
            authorization_occurred_at: fixture.now.clone(),
            content_digest: "baseline-content-digest".to_owned(),
            rendered_digest: "baseline-rendered-digest".to_owned(),
            idempotency_key: format!("approval-{}", baseline.id),
            created_at: fixture.now.clone(),
            updated_at: fixture.now.clone(),
        },
    )
    .await
    .expect("baseline approval");
    let activated = ProjectOrchestrationRepo::activate_project_execution_baseline(
        &fixture.db,
        ActivateProjectExecutionBaseline {
            approval_id: approval.id,
            expected_baseline_version: baseline.version + 2,
            expected_project_version: project_version,
            idempotency_key: format!("activation-{}", baseline.id),
            updated_at: fixture.now.clone(),
        },
    )
    .await
    .expect("baseline activation");
    assert_eq!(activated.lifecycle, "active");
}

#[tokio::test]
async fn baseline_shell_id_is_server_minted() {
    let fixture = charter_backed_project().await;
    let first = ProjectOrchestrationRepo::create_project_execution_baseline(
        &fixture.db,
        CreateProjectExecutionBaseline {
            project_id: fixture.project_id.clone(),
            created_at: fixture.now.clone(),
            updated_at: fixture.now.clone(),
        },
    )
    .await
    .expect("first baseline shell");
    let second = ProjectOrchestrationRepo::create_project_execution_baseline(
        &fixture.db,
        CreateProjectExecutionBaseline {
            project_id: fixture.project_id.clone(),
            created_at: fixture.now.clone(),
            updated_at: fixture.now.clone(),
        },
    )
    .await
    .expect("second baseline shell");
    assert!(
        validate_uuid_v4(&first.id),
        "baseline id must be a server-minted UUID v4, got {}",
        first.id
    );
    assert!(validate_uuid_v4(&second.id));
    assert_ne!(first.id, second.id);
    assert_eq!(first.project_id, fixture.project_id);
    assert_eq!(first.lifecycle, "draft");
}

#[tokio::test]
async fn activation_leaves_preplanned_task_governance_unchanged() {
    let fixture = charter_backed_project().await;

    // Baselines are optional traceability records. Activating one must not
    // mutate Task execution authority or attach Tasks implicitly.
    let preplanned = seed_task(&fixture, "Preplanned plan item", "todo").await;
    seed_prebaseline_governance(&fixture, &preplanned, "plan-item-1").await;
    let unrelated = seed_task(&fixture, "Unrelated plan item", "todo").await;
    seed_prebaseline_governance(&fixture, &unrelated, "plan-item-unrelated").await;
    let finished = seed_task(&fixture, "Finished plan item", "done").await;
    seed_prebaseline_governance(&fixture, &finished, "plan-item-2").await;

    activate_baseline(&fixture).await;

    let preplanned_row = sqlx::query(
        "SELECT baseline_id, baseline_revision_id, plan_item_id, milestone_id,
                charter_revision_id, runnable, provenance_json
         FROM project_task_governance WHERE task_id = ?",
    )
    .bind(&preplanned)
    .fetch_one(fixture.db.pool())
    .await
    .expect("preplanned governance row");
    assert_eq!(preplanned_row.get::<Option<String>, _>("baseline_id"), None);
    assert_eq!(
        preplanned_row.get::<Option<String>, _>("baseline_revision_id"),
        None
    );
    assert_eq!(
        preplanned_row
            .get::<Option<String>, _>("plan_item_id")
            .as_deref(),
        Some("plan-item-1")
    );
    assert_eq!(
        preplanned_row.get::<Option<String>, _>("milestone_id"),
        None
    );
    assert_eq!(
        preplanned_row
            .get::<Option<String>, _>("charter_revision_id")
            .as_deref(),
        Some(fixture.charter_revision_id.as_str())
    );
    assert_eq!(preplanned_row.get::<i64, _>("runnable"), 0);
    let provenance: serde_json::Value =
        serde_json::from_str(&preplanned_row.get::<String, _>("provenance_json"))
            .expect("preplanned provenance JSON");
    assert_eq!(provenance["baseline_pending"], serde_json::json!(true));

    let untouched =
        sqlx::query("SELECT baseline_id, runnable FROM project_task_governance WHERE task_id = ?")
            .bind(&unrelated)
            .fetch_one(fixture.db.pool())
            .await
            .expect("unrelated governance row");
    assert_eq!(untouched.get::<Option<String>, _>("baseline_id"), None);
    assert_eq!(untouched.get::<i64, _>("runnable"), 0);

    let terminal =
        sqlx::query("SELECT baseline_id, runnable FROM project_task_governance WHERE task_id = ?")
            .bind(&finished)
            .fetch_one(fixture.db.pool())
            .await
            .expect("terminal governance row");
    assert_eq!(terminal.get::<Option<String>, _>("baseline_id"), None);
    assert_eq!(terminal.get::<i64, _>("runnable"), 0);
}

#[tokio::test]
async fn activation_does_not_change_explicit_task_traceability_flags() {
    let fixture = charter_backed_project().await;
    let bound = seed_task(&fixture, "Bound preplanned", "todo").await;

    // Bind the governance row to the exact revision before activation, the
    // way an agent proposal against an approved-but-inactive baseline does.
    let baseline = ProjectOrchestrationRepo::create_project_execution_baseline(
        &fixture.db,
        CreateProjectExecutionBaseline {
            project_id: fixture.project_id.clone(),
            created_at: fixture.now.clone(),
            updated_at: fixture.now.clone(),
        },
    )
    .await
    .expect("baseline shell");
    let revision = ProjectOrchestrationRepo::create_project_execution_baseline_revision(
        &fixture.db,
        CreateProjectExecutionBaselineRevision {
            id: new_uuid_v4(),
            baseline_id: baseline.id.clone(),
            expected_baseline_version: baseline.version,
            base_revision: 0,
            base_revision_id: None,
            lifecycle: "proposed".to_owned(),
            charter_revision_id: fixture.charter_revision_id.clone(),
            document_revisions_json: "[]".to_owned(),
            plan_items_json: r#"[{"id":"plan-item-1"}]"#.to_owned(),
            milestone_id: Some(fixture.milestone_id.clone()),
            milestone_ids_json: format!("[\"{}\"]", fixture.milestone_id),
            milestone_definition_revision_ids_json: format!(
                "[\"{}\"]",
                fixture.milestone_definition_revision_id
            ),
            primary_milestone_id: Some(fixture.milestone_id.clone()),
            release_policy_json: "{}".to_owned(),
            release_policy_revision: "release-policy@1".to_owned(),
            release_policy_digest: "release-policy-digest".to_owned(),
            acceptance_matrix_json: "[]".to_owned(),
            capability_classes_json: "[]".to_owned(),
            risk_classes_json: "[]".to_owned(),
            adaptive_envelope_json: "{}".to_owned(),
            elevated_operations_json: "[]".to_owned(),
            exclusions_json: "[]".to_owned(),
            rollback_recovery_json: "{}".to_owned(),
            schema_version: "forge.project-orchestration/v1".to_owned(),
            render_version: "1".to_owned(),
            rendered_view: "# Baseline".to_owned(),
            content_digest: "baseline-content-digest".to_owned(),
            rendered_digest: "baseline-rendered-digest".to_owned(),
            source_refs_json: "[]".to_owned(),
            created_at: fixture.now.clone(),
        },
    )
    .await
    .expect("baseline revision");
    sqlx::query(
        "INSERT INTO project_task_governance
         (task_id, project_id, charter_revision_id, baseline_id,
          baseline_revision_id, plan_item_id, milestone_id,
          document_revisions_json, capability_class, risk_class, runnable,
          provenance_json, version, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, 'plan-item-1', ?, '[]', 'repository_write',
                 'low', 0, '{}', 1, ?, ?)",
    )
    .bind(&bound)
    .bind(&fixture.project_id)
    .bind(&fixture.charter_revision_id)
    .bind(&baseline.id)
    .bind(&revision.id)
    .bind(&fixture.milestone_id)
    .bind(&fixture.now)
    .bind(&fixture.now)
    .execute(fixture.db.pool())
    .await
    .expect("bound governance fixture");

    let project_version: i64 = sqlx::query_scalar("SELECT version FROM project WHERE id = ?")
        .bind(&fixture.project_id)
        .fetch_one(fixture.db.pool())
        .await
        .expect("project version");
    let approval = ProjectOrchestrationRepo::approve_project_execution_baseline(
        &fixture.db,
        ApproveProjectExecutionBaseline {
            id: new_uuid_v4(),
            baseline_id: baseline.id.clone(),
            revision_id: revision.id.clone(),
            expected_baseline_version: baseline.version + 1,
            expected_project_version: project_version,
            principal_type: "user".to_owned(),
            principal_id: fixture.project_id.clone(),
            authorization_basis: "explicit baseline approval".to_owned(),
            authorization_action: "project.execution_baseline.approve".to_owned(),
            explicit_event: "approve baseline".to_owned(),
            authorization_occurred_at: fixture.now.clone(),
            content_digest: "baseline-content-digest".to_owned(),
            rendered_digest: "baseline-rendered-digest".to_owned(),
            idempotency_key: format!("approval-{}", baseline.id),
            created_at: fixture.now.clone(),
            updated_at: fixture.now.clone(),
        },
    )
    .await
    .expect("baseline approval");
    ProjectOrchestrationRepo::activate_project_execution_baseline(
        &fixture.db,
        ActivateProjectExecutionBaseline {
            approval_id: approval.id,
            expected_baseline_version: baseline.version + 2,
            expected_project_version: project_version,
            idempotency_key: format!("activation-{}", baseline.id),
            updated_at: fixture.now.clone(),
        },
    )
    .await
    .expect("baseline activation");

    let runnable: i64 =
        sqlx::query_scalar("SELECT runnable FROM project_task_governance WHERE task_id = ?")
            .bind(&bound)
            .fetch_one(fixture.db.pool())
            .await
            .expect("bound governance runnable");
    assert_eq!(runnable, 0);
}

#[tokio::test]
async fn activation_sets_the_primary_milestone_pointer_when_missing() {
    let fixture = charter_backed_project().await;
    let before: Option<String> =
        sqlx::query_scalar("SELECT primary_milestone_id FROM project WHERE id = ?")
            .bind(&fixture.project_id)
            .fetch_one(fixture.db.pool())
            .await
            .expect("primary pointer before activation");
    assert_eq!(before, None);

    activate_baseline(&fixture).await;

    let after: Option<String> =
        sqlx::query_scalar("SELECT primary_milestone_id FROM project WHERE id = ?")
            .bind(&fixture.project_id)
            .fetch_one(fixture.db.pool())
            .await
            .expect("primary pointer after activation");
    assert_eq!(after, Some(fixture.milestone_id.clone()));
    let milestone_lifecycle: String =
        sqlx::query_scalar("SELECT lifecycle FROM project_milestone WHERE id = ?")
            .bind(&fixture.milestone_id)
            .fetch_one(fixture.db.pool())
            .await
            .expect("milestone lifecycle");
    assert_eq!(milestone_lifecycle, "active");
}
