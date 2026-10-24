//! Milestone authorization, evidence, and repository context cases.

use super::*;
use db::{create_sqlite_pool, run_migrations};

#[test]
fn principal_identity_ignores_the_display_label() {
    let stored = PrincipalRef {
        kind: PrincipalKind::User,
        id: "user-1".to_owned(),
        display_name: None,
    };
    let sent = PrincipalRef {
        kind: PrincipalKind::User,
        id: "user-1".to_owned(),
        display_name: Some("E2E Default User".to_owned()),
    };
    // The row keeps only kind and id, so a caller that sent the label the
    // UI displayed must still match the author rebuilt from those columns.
    assert!(same_principal(&stored, &sent));

    let other = PrincipalRef {
        kind: PrincipalKind::User,
        id: "user-2".to_owned(),
        display_name: None,
    };
    assert!(!same_principal(&stored, &other));

    let agent = PrincipalRef {
        kind: PrincipalKind::Agent,
        id: "user-1".to_owned(),
        display_name: None,
    };
    assert!(!same_principal(&stored, &agent));
}

#[test]
fn result_and_principal_names_are_closed() {
    assert_eq!(readiness_result_name(ReadinessResult::Ready), "ready");
    assert_eq!(principal_kind_name(PrincipalKind::Reviewer), "reviewer");
    assert_eq!(
        evidence_availability_name(EvidenceAvailability::Purged),
        "purged"
    );
}

#[test]
fn corrupt_immutable_json_is_reported_instead_of_defaulted() {
    let error = parse_json_required::<Vec<String>>("{", "release changelog")
        .expect_err("corrupt immutable JSON must fail closed");
    match error {
        crate::ServiceError::InvalidOperation { message } => {
            assert!(message.contains("corrupt immutable release changelog JSON"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn authorization_timestamps_are_rfc3339_and_bounded() {
    let now = Utc::now().to_rfc3339();
    assert!(well_formed_authorization_timestamp(&now));
    assert!(valid_authorization_timestamp(&now));
    assert!(!well_formed_authorization_timestamp("not-a-timestamp"));
    assert!(!well_formed_authorization_timestamp(&format!(" {now}")));
    let old = (Utc::now() - chrono::Duration::hours(49)).to_rfc3339();
    assert!(well_formed_authorization_timestamp(&old));
    assert!(!valid_authorization_timestamp(&old));
}

#[test]
fn persisted_enum_values_fail_closed() {
    assert!(principal_kind("unknown").is_err());
    assert!(check_status("unknown").is_err());
    assert!(evidence_kind(Some("unknown")).is_err());
    assert!(evidence_availability("unknown").is_err());
    assert!(release_evidence_tombstone("unknown").is_err());
}

#[test]
fn purged_release_evidence_is_projected_unavailable() {
    assert_eq!(
        release_evidence_tombstone("purged").expect("purge overlay"),
        api_types::ReleaseEvidenceAvailability::EvidenceUnavailable
    );
}

#[tokio::test]
async fn repository_context_builds_without_remote_url() {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let now = "2026-09-30T00:00:00Z";
    let runtime = MilestoneRuntime::new(Arc::clone(&db));
    for (index, remote_url) in [None, Some(""), Some(" \t\r\n"), Some("\u{2003}")]
        .into_iter()
        .enumerate()
    {
        let project_id = format!("project-local-{index}");
        let repo_id = format!("repo-local-{index}");
        let task_id = format!("task-local-{index}");
        sqlx::query(
            "INSERT INTO project (id, name, settings, workflow_definition, created_at, updated_at)
                 VALUES (?, 'Local Project', '{}', '{}', ?, ?)",
        )
        .bind(&project_id)
        .bind(now)
        .bind(now)
        .execute(db.pool())
        .await
        .expect("project inserts");
        let repo = db::RepoRepo::create(
            &*db,
            db::CreateRepo {
                id: repo_id.clone(),
                project_id: project_id.clone(),
                name: "Local Repo".to_owned(),
                remote_url: remote_url.map(str::to_owned),
                local_path: Some("/tmp/local-repo".to_owned()),
                default_branch: "main".to_owned(),
                created_at: now.to_owned(),
                updated_at: now.to_owned(),
            },
        )
        .await
        .expect("repo creates");
        assert_eq!(repo.remote_url, None);
        sqlx::query("UPDATE project SET primary_repo_id = ? WHERE id = ?")
            .bind(&repo_id)
            .bind(&project_id)
            .execute(db.pool())
            .await
            .expect("primary repo selects");
        sqlx::query(
            "INSERT INTO task (id, project_id, title, task_type, status, created_at, updated_at)
                 VALUES (?, ?, 'Local Task', 'task', 'done', ?, ?)",
        )
        .bind(&task_id)
        .bind(&project_id)
        .bind(now)
        .bind(now)
        .execute(db.pool())
        .await
        .expect("task inserts");
        for raw_remote in [None, remote_url] {
            sqlx::query("UPDATE repo SET remote_url = ? WHERE id = ?")
                .bind(raw_remote)
                .bind(&repo_id)
                .execute(db.pool())
                .await
                .expect("raw remote seeds");
            let mut tx = db.pool().begin().await.expect("transaction begins");
            let contexts = runtime
                .commit_build_check_context_in_tx(
                    &mut tx,
                    &project_id,
                    &[ReadinessTaskState {
                        task_id: task_id.clone(),
                        version: 1,
                        task_type: "task".to_owned(),
                        state: "done".to_owned(),
                        observed_at: now.to_owned(),
                    }],
                )
                .await
                .expect("local repository context builds");
            tx.rollback().await.expect("transaction rolls back");
            assert_eq!(contexts.len(), 1);
            let reference: RepositoryContextReference =
                serde_json::from_str(&contexts[0]).expect("context decodes");
            assert_eq!(reference.repository_id, repo_id);
            assert_eq!(reference.remote_url, None);
            validate_repository_context_reference(&reference).expect("local context validates");
            let mut incomplete = reference;
            incomplete.repository_name.clear();
            assert!(validate_repository_context_reference(&incomplete).is_err());
        }
    }
}

#[tokio::test]
async fn repository_context_digest_includes_stored_repository_kind() {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let observed_at = "2026-09-30T10:11:12Z";
    sqlx::query(
        "INSERT INTO project
             (id, name, settings, workflow_definition, primary_repo_id, created_at, updated_at)
             VALUES ('project-digest', 'Digest Project', '{}', '{}', NULL, ?, ?)",
    )
    .bind(observed_at)
    .bind(observed_at)
    .execute(db.pool())
    .await
    .expect("project inserts");
    sqlx::query(
        "INSERT INTO repo
             (id, project_id, name, remote_url, work_mode, default_branch, created_at, updated_at)
             VALUES ('repo-digest', 'project-digest', 'Digest Repo',
                     'https://example.test/digest.git', 'pull_request', 'release', ?, ?)",
    )
    .bind(observed_at)
    .bind(observed_at)
    .execute(db.pool())
    .await
    .expect("repository inserts");
    sqlx::query("UPDATE project SET primary_repo_id = 'repo-digest' WHERE id = 'project-digest'")
        .execute(db.pool())
        .await
        .expect("primary repository selects");
    sqlx::query(
        "INSERT INTO task
             (id, project_id, title, task_type, status, created_at, updated_at)
             VALUES ('task-digest', 'project-digest', 'Digest Task', 'task', 'done', ?, ?)",
    )
    .bind(observed_at)
    .bind(observed_at)
    .execute(db.pool())
    .await
    .expect("task inserts");

    let runtime = MilestoneRuntime::new(Arc::clone(&db));
    let mut tx = db.pool().begin().await.expect("transaction begins");
    let contexts = runtime
        .commit_build_check_context_in_tx(
            &mut tx,
            "project-digest",
            &[ReadinessTaskState {
                task_id: "task-digest".to_owned(),
                version: 3,
                task_type: "task".to_owned(),
                state: "done".to_owned(),
                observed_at: observed_at.to_owned(),
            }],
        )
        .await
        .expect("repository context builds");
    tx.rollback().await.expect("transaction rolls back");

    let reference: RepositoryContextReference =
        serde_json::from_str(&contexts[0]).expect("repository context decodes");
    assert_eq!(reference.repository_kind, "pull_request");
    assert_eq!(
        canonical_digest_with_schema(MILESTONE_RELEASE_DIGEST_SCHEMA_VERSION, &reference)
            .expect("release reference digest computes"),
        "58b2fd11c4b612dda1dd63aca95a472fffc117d212c79cc95e0612aaaa587f47"
    );
}

#[tokio::test]
async fn repository_context_uses_execution_workspace_after_project_reselection() {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let now = "2026-09-10T00:00:00Z";
    sqlx::query(
        "INSERT INTO project
             (id, name, settings, workflow_definition, created_at, updated_at)
             VALUES ('project-provenance', 'Project', '{}', '{}', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(db.pool())
    .await
    .expect("project inserts");
    for (id, name, branch) in [
        ("repo-attempt", "Attempt Repo", "main"),
        ("repo-current", "Current Repo", "trunk"),
    ] {
        sqlx::query(
                "INSERT INTO repo
                 (id, project_id, name, remote_url, work_mode, default_branch, created_at, updated_at)
                 VALUES (?, 'project-provenance', ?, ?, 'direct_merge', ?, ?, ?)",
            )
            .bind(id)
            .bind(name)
            .bind(format!("https://example.com/{id}.git"))
            .bind(branch)
            .bind(now)
            .bind(now)
            .execute(db.pool())
            .await
            .expect("repo inserts");
    }
    sqlx::query(
        "UPDATE project SET primary_repo_id = 'repo-current' WHERE id = 'project-provenance'",
    )
    .execute(db.pool())
    .await
    .expect("current project repo selects");
    sqlx::query(
        "INSERT INTO task
             (id, project_id, title, task_type, status, created_at, updated_at)
             VALUES ('task-provenance', 'project-provenance', 'Task', 'task', 'done', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(db.pool())
    .await
    .expect("task inserts");
    sqlx::query(
        "INSERT INTO workspace
             (id, task_id, repo_id, worktree_path, branch, status, created_at, updated_at)
             VALUES ('workspace-provenance', 'task-provenance', 'repo-attempt', '/tmp/provenance',
                     'forge/task-provenance', 'ready', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(db.pool())
    .await
    .expect("workspace inserts");
    sqlx::query(
        "INSERT INTO execution
             (id, task_id, role, status, workspace_id, created_at, updated_at)
             VALUES ('execution-provenance', 'task-provenance', 'executor', 'completed',
                     'workspace-provenance', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(db.pool())
    .await
    .expect("execution inserts");

    let service = MilestoneRuntime::new(Arc::clone(&db));
    let mut tx = db.pool().begin().await.expect("transaction begins");
    let contexts = service
        .commit_build_check_context_in_tx(
            &mut tx,
            "project-provenance",
            &[ReadinessTaskState {
                task_id: "task-provenance".to_owned(),
                version: 1,
                task_type: "task".to_owned(),
                state: "done".to_owned(),
                observed_at: now.to_owned(),
            }],
        )
        .await
        .expect("repository context builds");
    tx.rollback().await.expect("transaction rolls back");

    let reference: RepositoryContextReference =
        serde_json::from_str(&contexts[0]).expect("repository context decodes");
    assert_eq!(reference.repository_id, "repo-attempt");
    assert_eq!(reference.repository_name, "Attempt Repo");
    assert_eq!(reference.default_branch, "main");
}

#[test]
fn blocker_bucket_accepts_typed_check_and_evidence_reasons() {
    let record = ProjectMilestoneRecord {
        id: "milestone-1".to_owned(),
        project_id: "project-1".to_owned(),
        milestone_sequence: 1,
        milestone_key: "M001".to_owned(),
        display_label: None,
        current_definition_revision_id: Some("revision-1".to_owned()),
        lifecycle: "active".to_owned(),
        blocker_reason_json: serde_json::to_string(&vec![
            MilestoneProjectionReason {
                kind: MilestoneProjectionReasonKind::CheckFailed,
                code: "check_missing".to_owned(),
                message: "required check has no result".to_owned(),
                source_ids: vec!["check-1".to_owned()],
            },
            MilestoneProjectionReason {
                kind: MilestoneProjectionReasonKind::EvidenceUnavailable,
                code: "evidence_missing".to_owned(),
                message: "required evidence is unavailable".to_owned(),
                source_ids: vec!["evidence-1".to_owned()],
            },
        ])
        .expect("reasons serialize"),
        stale_reason_json: "[]".to_owned(),
        reconciliation_reason_json: "[]".to_owned(),
        version: 1,
        created_at: "2026-08-14T00:00:00Z".to_owned(),
        updated_at: "2026-08-14T00:00:00Z".to_owned(),
    };
    let reasons = projection_reasons_from_record(&record).expect("typed blockers decode");
    assert_eq!(reasons.len(), 2);
}
