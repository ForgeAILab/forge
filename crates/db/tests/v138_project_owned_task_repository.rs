use db::{create_sqlite_pool, run_migrations_from};
use sqlx::{Row, SqlitePool};
use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

const NOW: &str = "2026-09-10T00:00:00Z";
const PRIMARY_REPO_ID: &str = "v138-repo-primary";
const EXTRA_REPO_ID: &str = "v138-repo-extra";
const OTHER_REPO_ID: &str = "v138-repo-other";
const CROSS_PROJECT_REPO_ID: &str = "v138-repo-cross-project-owned";
const AGENT_ID: &str = "v138-worker";
const WRITE_CAPABILITY_DIGEST: &str =
    "sha256:eeb061a14ab862e1a7b16989ef637293ba538f46122ff28b30313d330dbae4a8";

type TaskSnapshotRow = (
    String,
    String,
    Option<String>,
    String,
    Option<String>,
    Option<String>,
    i64,
);

fn unique_temp_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("forge-{name}-{}-{nanos}", std::process::id()))
}

fn migration_version(filename: &str) -> Option<i64> {
    filename.strip_prefix('V')?.split_once("__")?.0.parse().ok()
}

fn copy_migrations_up_to(max_version: i64, destination: &Path) {
    let source_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    for entry in fs::read_dir(source_dir).expect("migration directory reads") {
        let entry = entry.expect("migration entry reads");
        let path = entry.path();
        let Some(filename) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(version) = migration_version(filename) else {
            continue;
        };
        if version <= max_version {
            fs::copy(&path, destination.join(filename)).expect("migration copies");
        }
    }
}

fn copy_v138(destination: &Path) {
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("migrations")
        .join("V138__project_owned_task_repository.sql");
    fs::copy(
        source,
        destination.join("V138__project_owned_task_repository.sql"),
    )
    .expect("V138 migration copies");
}

#[allow(clippy::too_many_arguments)]
async fn insert_task(
    pool: &SqlitePool,
    id: &str,
    project_id: &str,
    repo_id: Option<&str>,
    parent_task_id: Option<&str>,
    status: &str,
    archived_at: Option<&str>,
    deleted_at: Option<&str>,
) {
    sqlx::query(
        "INSERT INTO task (
            id, project_id, repo_id, parent_task_id, assignee_type, assignee_id,
            title, task_type, status, subtask_order, archived_at, deleted_at,
            version, created_at, updated_at
         ) VALUES (?, ?, ?, ?, 'agent', ?, ?, 'task', ?, ?, ?, ?, 3, ?, ?)",
    )
    .bind(id)
    .bind(project_id)
    .bind(repo_id)
    .bind(parent_task_id)
    .bind(AGENT_ID)
    .bind(format!("Fixture {id}"))
    .bind(status)
    .bind(parent_task_id.map(|_| 1_i64))
    .bind(archived_at)
    .bind(deleted_at)
    .bind(NOW)
    .bind(NOW)
    .execute(pool)
    .await
    .expect("pre-V138 Task inserts");
}

async fn insert_workspace_and_execution(
    pool: &SqlitePool,
    task_id: &str,
    repo_id: &str,
    suffix: &str,
) {
    sqlx::query(
        "INSERT INTO workspace (
            id, task_id, repo_id, worktree_path, branch, status,
            before_sha, created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, 'ready', 'before-sha', ?, ?)",
    )
    .bind(format!("v138-workspace-{suffix}"))
    .bind(task_id)
    .bind(repo_id)
    .bind(format!("/tmp/forge-v138-{suffix}"))
    .bind(format!("forge/v138-{suffix}"))
    .bind(NOW)
    .bind(NOW)
    .execute(pool)
    .await
    .expect("Workspace inserts");

    sqlx::query(
        "INSERT INTO execution (
            id, task_id, agent_id, role, status, workspace_id,
            created_at, updated_at
         ) VALUES (?, ?, ?, 'executor', 'running', ?, ?, ?)",
    )
    .bind(format!("v138-execution-{suffix}"))
    .bind(task_id)
    .bind(AGENT_ID)
    .bind(format!("v138-workspace-{suffix}"))
    .bind(NOW)
    .bind(NOW)
    .execute(pool)
    .await
    .expect("Execution inserts");
}

async fn insert_active_lease(
    pool: &SqlitePool,
    id: &str,
    task_id: &str,
    project_id: &str,
    execution_suffix: &str,
    repository_binding_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO workspace_lease (
            id, project_id, task_id, task_version, execution_id,
            operation_idempotency_key, repository_binding_id, base_ref, role,
            capabilities_json, assigned_principal_type, assigned_principal_id,
            capability_profile_revision, capability_profile_digest,
            issuing_principal_type, issuing_principal_id, status,
            issued_at, expires_at, version, created_at, updated_at
         ) VALUES (
            ?, ?, ?, 3, ?, ?, ?, 'main', 'worker', '[\"repository_write\"]',
            'agent', ?, 'forge.capability-profile/v1', ?,
            'system', 'task-service-scheduler', 'active', ?,
            '2026-09-10T01:00:00Z', 1, ?, ?
         )",
    )
    .bind(id)
    .bind(project_id)
    .bind(task_id)
    .bind(format!("v138-execution-{execution_suffix}"))
    .bind(format!("v138-operation-{id}"))
    .bind(repository_binding_id)
    .bind(AGENT_ID)
    .bind(WRITE_CAPABILITY_DIGEST)
    .bind(NOW)
    .bind(NOW)
    .bind(NOW)
    .execute(pool)
    .await
    .map(|_| ())
}

#[tokio::test]
async fn v138_preserves_task_history_and_moves_lease_authority_to_the_project() {
    let migration_dir = unique_temp_path("v138-project-repository-migrations");
    fs::create_dir_all(&migration_dir).expect("migration temp directory creates");
    copy_migrations_up_to(137, &migration_dir);

    let db_path = unique_temp_path("v138-project-repository-db").with_extension("db");
    let url = format!("sqlite://{}", db_path.display());
    let pool = create_sqlite_pool(&url).await.expect("file-backed pool");
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("pre-V138 migrations apply");

    for project_id in [
        "v138-project",
        "v138-project-without-primary",
        "v138-project-cross-primary",
    ] {
        sqlx::query(
            "INSERT INTO project (
                id, name, settings, workflow_definition, created_at, updated_at
             ) VALUES (?, ?, '{}', '{}', ?, ?)",
        )
        .bind(project_id)
        .bind(project_id)
        .bind(NOW)
        .bind(NOW)
        .execute(&pool)
        .await
        .expect("Project inserts");
    }

    for (id, project_id) in [
        (PRIMARY_REPO_ID, "v138-project"),
        (EXTRA_REPO_ID, "v138-project"),
        (OTHER_REPO_ID, "v138-project-without-primary"),
        (CROSS_PROJECT_REPO_ID, "v138-project-cross-primary"),
    ] {
        sqlx::query(
            "INSERT INTO repo (
                id, project_id, name, remote_url, local_path, work_mode,
                default_branch, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, 'direct_merge', 'main', ?, ?)",
        )
        .bind(id)
        .bind(project_id)
        .bind(id)
        .bind(format!("https://example.test/{id}.git"))
        .bind(format!("/tmp/{id}"))
        .bind(NOW)
        .bind(NOW)
        .execute(&pool)
        .await
        .expect("Repo inserts");
    }
    sqlx::query(
        "UPDATE project SET primary_repo_id = ?, version = version + 1, updated_at = ?
         WHERE id = 'v138-project'",
    )
    .bind(PRIMARY_REPO_ID)
    .bind(NOW)
    .execute(&pool)
    .await
    .expect("Project primary Repo selects");
    sqlx::query(
        "UPDATE project SET primary_repo_id = ?, version = version + 1, updated_at = ?
         WHERE id = 'v138-project-cross-primary'",
    )
    .bind(PRIMARY_REPO_ID)
    .bind(NOW)
    .execute(&pool)
    .await
    .expect("legacy cross-Project primary Repo selects");

    sqlx::query(
        "INSERT INTO agent_identity (
            id, name, status, version, created_at, updated_at
         ) VALUES (?, 'V138 worker', 'busy', 1, ?, ?)",
    )
    .bind(AGENT_ID)
    .bind(NOW)
    .bind(NOW)
    .execute(&pool)
    .await
    .expect("Agent identity inserts");

    insert_task(
        &pool,
        "v138-task-null",
        "v138-project",
        None,
        None,
        "todo",
        None,
        None,
    )
    .await;
    insert_task(
        &pool,
        "v138-task-matching",
        "v138-project",
        Some(PRIMARY_REPO_ID),
        None,
        "in_progress",
        None,
        None,
    )
    .await;
    insert_task(
        &pool,
        "v138-task-stale",
        "v138-project",
        Some(EXTRA_REPO_ID),
        None,
        "in_review",
        None,
        None,
    )
    .await;
    insert_task(
        &pool,
        "v138-task-archived",
        "v138-project",
        Some(PRIMARY_REPO_ID),
        None,
        "done",
        Some("2026-09-10T00:10:00Z"),
        None,
    )
    .await;
    insert_task(
        &pool,
        "v138-task-deleted",
        "v138-project",
        Some(EXTRA_REPO_ID),
        None,
        "done",
        None,
        Some("2026-09-10T00:20:00Z"),
    )
    .await;
    insert_task(
        &pool,
        "v138-task-child",
        "v138-project",
        None,
        Some("v138-task-null"),
        "todo",
        None,
        None,
    )
    .await;
    for task_id in ["v138-task-extra-probe", "v138-task-stale-workspace-probe"] {
        insert_task(
            &pool,
            task_id,
            "v138-project",
            Some(EXTRA_REPO_ID),
            None,
            "in_progress",
            None,
            None,
        )
        .await;
    }
    insert_task(
        &pool,
        "v138-task-without-primary",
        "v138-project-without-primary",
        Some(OTHER_REPO_ID),
        None,
        "in_progress",
        None,
        None,
    )
    .await;
    insert_task(
        &pool,
        "v138-task-no-primary-probe",
        "v138-project-without-primary",
        Some(OTHER_REPO_ID),
        None,
        "in_progress",
        None,
        None,
    )
    .await;
    insert_task(
        &pool,
        "v138-task-cross-primary-probe",
        "v138-project-cross-primary",
        Some(PRIMARY_REPO_ID),
        None,
        "in_progress",
        None,
        None,
    )
    .await;

    for (task_id, depends_on_id) in [
        ("v138-task-child", "v138-task-matching"),
        ("v138-task-child", "v138-task-stale"),
    ] {
        sqlx::query(
            "INSERT INTO task_dependency (task_id, depends_on_id, created_at)
             VALUES (?, ?, ?)",
        )
        .bind(task_id)
        .bind(depends_on_id)
        .bind(NOW)
        .execute(&pool)
        .await
        .expect("Task dependency inserts");
    }

    for (task_id, repo_id, suffix) in [
        ("v138-task-matching", PRIMARY_REPO_ID, "matching"),
        ("v138-task-stale", EXTRA_REPO_ID, "stale"),
        (
            "v138-task-without-primary",
            OTHER_REPO_ID,
            "without-primary",
        ),
        (
            "v138-task-no-primary-probe",
            OTHER_REPO_ID,
            "no-primary-probe",
        ),
        ("v138-task-extra-probe", EXTRA_REPO_ID, "extra-probe"),
        (
            "v138-task-stale-workspace-probe",
            EXTRA_REPO_ID,
            "stale-workspace-probe",
        ),
        (
            "v138-task-cross-primary-probe",
            PRIMARY_REPO_ID,
            "cross-primary-probe",
        ),
    ] {
        insert_workspace_and_execution(&pool, task_id, repo_id, suffix).await;
    }

    insert_active_lease(
        &pool,
        "v138-lease-matching",
        "v138-task-matching",
        "v138-project",
        "matching",
        PRIMARY_REPO_ID,
    )
    .await
    .expect("matching pre-V138 lease inserts");
    insert_active_lease(
        &pool,
        "v138-lease-stale",
        "v138-task-stale",
        "v138-project",
        "stale",
        EXTRA_REPO_ID,
    )
    .await
    .expect("stale pre-V138 lease inserts under legacy Task authority");
    insert_active_lease(
        &pool,
        "v138-lease-without-primary",
        "v138-task-without-primary",
        "v138-project-without-primary",
        "without-primary",
        OTHER_REPO_ID,
    )
    .await
    .expect("pre-V138 lease inserts without a Project primary Repo");

    for (suffix, task_id) in [
        ("matching", "v138-task-matching"),
        ("stale", "v138-task-stale"),
    ] {
        sqlx::query(
            "INSERT INTO review (
                id, task_id, execution_id, attempt_number, status,
                step_results_json, started_at, created_at, updated_at
             ) VALUES (?, ?, ?, 1, 'running', '[]', ?, ?, ?)",
        )
        .bind(format!("v138-review-{suffix}"))
        .bind(task_id)
        .bind(format!("v138-execution-{suffix}"))
        .bind(NOW)
        .bind(NOW)
        .bind(NOW)
        .execute(&pool)
        .await
        .expect("Review inserts");
    }
    sqlx::query(
        "INSERT INTO execution_review_contract (
            execution_id, task_id, contract_digest, source_digest,
            contract_json, created_at
         ) VALUES (
            'v138-execution-stale', 'v138-task-stale',
            'v138-contract-digest', 'v138-source-digest', '{}', ?
         )",
    )
    .bind(NOW)
    .execute(&pool)
    .await
    .expect("Review contract inserts");
    sqlx::query(
        "INSERT INTO execution_review_assessment (
            execution_id, conformance_json, created_at
         ) VALUES ('v138-execution-stale', '{}', ?)",
    )
    .bind(NOW)
    .execute(&pool)
    .await
    .expect("Review assessment inserts");

    let legacy_binding_counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT
            SUM(repo_id IS NULL),
            SUM(repo_id = ?),
            SUM(repo_id = ?)
         FROM task WHERE project_id = 'v138-project'",
    )
    .bind(PRIMARY_REPO_ID)
    .bind(EXTRA_REPO_ID)
    .fetch_one(&pool)
    .await
    .expect("legacy Task Repo binding counts load");
    assert_eq!(legacy_binding_counts, (2, 2, 4));

    let task_snapshot_before: Vec<TaskSnapshotRow> = sqlx::query_as(
        "SELECT id, project_id, parent_task_id, status, archived_at, deleted_at, version
         FROM task ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("pre-V138 Task snapshot loads");
    let related_counts_before: (i64, i64, i64, i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
                (SELECT COUNT(*) FROM repo),
                (SELECT COUNT(*) FROM task_dependency),
                (SELECT COUNT(*) FROM workspace),
                (SELECT COUNT(*) FROM execution),
                (SELECT COUNT(*) FROM workspace_lease),
                (SELECT COUNT(*) FROM review),
                (SELECT COUNT(*) FROM execution_review_contract),
                (SELECT COUNT(*) FROM execution_review_assessment)",
    )
    .fetch_one(&pool)
    .await
    .expect("pre-V138 related row counts load");

    copy_v138(&migration_dir);
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("V138 migration applies");
    pool.close().await;

    let pool = create_sqlite_pool(&url)
        .await
        .expect("migrated file-backed database reopens");
    let repo_column_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM pragma_table_info('task') WHERE name = 'repo_id'")
            .fetch_one(&pool)
            .await
            .expect("Task schema inspects");
    assert_eq!(repo_column_count, 0, "Task.repo_id is removed");
    let legacy_index_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master
         WHERE type = 'index' AND name = 'idx_task_repo'",
    )
    .fetch_one(&pool)
    .await
    .expect("legacy Task Repo index lookup");
    assert_eq!(legacy_index_count, 0);
    for object in [
        "task_insert_requires_assignee_id",
        "task_board_revision_after_insert",
        "task_board_revision_after_delete",
        "task_board_revision_after_update",
        "idx_task_status_project",
        "idx_task_parent",
        "idx_task_assignee",
        "idx_task_parent_subtask_order",
        "idx_task_project_archived",
        "idx_task_project_automation",
    ] {
        let object_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE name = ? AND type IN ('index', 'trigger')",
        )
        .bind(object)
        .fetch_one(&pool)
        .await
        .expect("retained Task schema object lookup");
        assert_eq!(object_count, 1, "Task schema object {object} survives");
    }

    let lease_guard_sql: Vec<(String, String)> = sqlx::query_as(
        "SELECT name, sql FROM sqlite_master
         WHERE type = 'trigger'
           AND name IN (
               'workspace_lease_scope_guard_insert',
               'workspace_lease_active_renewal_guard'
           )
         ORDER BY name",
    )
    .fetch_all(&pool)
    .await
    .expect("Workspace lease guard definitions load");
    assert_eq!(lease_guard_sql.len(), 2);
    for (name, sql) in lease_guard_sql {
        assert!(
            sql.contains("p.primary_repo_id = NEW.repository_binding_id"),
            "{name} resolves repository authority from the Project"
        );
        assert!(
            sql.contains("w.repo_id = NEW.repository_binding_id"),
            "{name} validates attempt Workspace provenance"
        );
        assert!(
            !sql.contains("t.repo_id"),
            "{name} must not retain legacy Task repository authority"
        );
    }

    let task_snapshot_after: Vec<TaskSnapshotRow> = sqlx::query_as(
        "SELECT id, project_id, parent_task_id, status, archived_at, deleted_at, version
         FROM task ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("post-V138 Task snapshot loads");
    assert_eq!(task_snapshot_after, task_snapshot_before);

    let related_counts_after: (i64, i64, i64, i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
                (SELECT COUNT(*) FROM repo),
                (SELECT COUNT(*) FROM task_dependency),
                (SELECT COUNT(*) FROM workspace),
                (SELECT COUNT(*) FROM execution),
                (SELECT COUNT(*) FROM workspace_lease),
                (SELECT COUNT(*) FROM review),
                (SELECT COUNT(*) FROM execution_review_contract),
                (SELECT COUNT(*) FROM execution_review_assessment)",
    )
    .fetch_one(&pool)
    .await
    .expect("post-V138 related row counts load");
    assert_eq!(related_counts_after, related_counts_before);

    let dependencies: Vec<(String, String)> = sqlx::query_as(
        "SELECT task_id, depends_on_id FROM task_dependency
         WHERE task_id = 'v138-task-child' ORDER BY depends_on_id",
    )
    .fetch_all(&pool)
    .await
    .expect("Task dependencies load");
    assert_eq!(
        dependencies,
        vec![
            (
                "v138-task-child".to_owned(),
                "v138-task-matching".to_owned()
            ),
            ("v138-task-child".to_owned(), "v138-task-stale".to_owned()),
        ]
    );

    insert_workspace_and_execution(&pool, "v138-task-null", PRIMARY_REPO_ID, "formerly-null").await;
    insert_active_lease(
        &pool,
        "v138-lease-formerly-null",
        "v138-task-null",
        "v138-project",
        "formerly-null",
        PRIMARY_REPO_ID,
    )
    .await
    .expect("a formerly unbound Task leases the Project primary Repo");

    let extra_repo_lease = insert_active_lease(
        &pool,
        "v138-lease-extra-probe",
        "v138-task-extra-probe",
        "v138-project",
        "extra-probe",
        EXTRA_REPO_ID,
    )
    .await;
    let extra_repo_error = extra_repo_lease.expect_err("an unselected extra Repo is rejected");
    assert!(
        extra_repo_error
            .to_string()
            .contains("Workspace lease Task is cross-Project or stale"),
        "unexpected extra Repo rejection: {extra_repo_error}"
    );

    let stale_workspace_reuse = insert_active_lease(
        &pool,
        "v138-lease-stale-workspace-probe",
        "v138-task-stale-workspace-probe",
        "v138-project",
        "stale-workspace-probe",
        PRIMARY_REPO_ID,
    )
    .await;
    let stale_workspace_error =
        stale_workspace_reuse.expect_err("an old-Repo Workspace is rejected");
    assert!(
        stale_workspace_error
            .to_string()
            .contains("Workspace lease execution is not Task-scoped"),
        "unexpected stale Workspace rejection: {stale_workspace_error}"
    );

    let no_primary_lease = insert_active_lease(
        &pool,
        "v138-lease-no-primary-probe",
        "v138-task-no-primary-probe",
        "v138-project-without-primary",
        "no-primary-probe",
        OTHER_REPO_ID,
    )
    .await;
    let no_primary_error =
        no_primary_lease.expect_err("a Project without a primary Repo is rejected");
    assert!(
        no_primary_error
            .to_string()
            .contains("Workspace lease Task is cross-Project or stale"),
        "unexpected missing primary Repo rejection: {no_primary_error}"
    );

    let cross_primary_lease = insert_active_lease(
        &pool,
        "v138-lease-cross-primary-probe",
        "v138-task-cross-primary-probe",
        "v138-project-cross-primary",
        "cross-primary-probe",
        PRIMARY_REPO_ID,
    )
    .await;
    let cross_primary_error =
        cross_primary_lease.expect_err("a cross-Project primary Repo is rejected");
    assert!(
        cross_primary_error
            .to_string()
            .contains("Workspace lease Task is cross-Project or stale"),
        "unexpected cross-Project primary Repo rejection: {cross_primary_error}"
    );

    sqlx::query(
        "UPDATE workspace_lease
         SET expires_at = '2026-09-10T02:00:00Z',
             updated_at = '2026-09-10T00:30:00Z',
             version = version + 1
         WHERE id = 'v138-lease-matching'",
    )
    .execute(&pool)
    .await
    .expect("lease on the current Project Repo renews");

    for lease_id in ["v138-lease-stale", "v138-lease-without-primary"] {
        let renewal = sqlx::query(
            "UPDATE workspace_lease
             SET expires_at = '2026-09-10T02:00:00Z',
                 updated_at = '2026-09-10T00:30:00Z',
                 version = version + 1
             WHERE id = ?",
        )
        .bind(lease_id)
        .execute(&pool)
        .await;
        assert!(
            renewal.is_err(),
            "lease {lease_id} must not renew without current Project Repo authority"
        );
    }

    sqlx::query("DELETE FROM repo WHERE id = ?")
        .bind(EXTRA_REPO_ID)
        .execute(&pool)
        .await
        .expect("unselected extra Repo deletes");
    let formerly_bound_tasks: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task
         WHERE id IN ('v138-task-stale', 'v138-task-deleted')",
    )
    .fetch_one(&pool)
    .await
    .expect("Tasks formerly bound to the deleted Repo count");
    assert_eq!(
        formerly_bound_tasks, 2,
        "deleting a Repo no longer cascades into Task history"
    );
    let stale_execution_workspace: Option<String> =
        sqlx::query_scalar("SELECT workspace_id FROM execution WHERE id = 'v138-execution-stale'")
            .fetch_one(&pool)
            .await
            .expect("historical Execution remains");
    assert_eq!(stale_execution_workspace, None);
    let stale_history_counts: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM workspace_lease WHERE id = 'v138-lease-stale'),
            (SELECT COUNT(*) FROM review WHERE id = 'v138-review-stale'),
            (SELECT COUNT(*) FROM execution_review_contract
             WHERE execution_id = 'v138-execution-stale'),
            (SELECT COUNT(*) FROM execution_review_assessment
             WHERE execution_id = 'v138-execution-stale'),
            (SELECT COUNT(*) FROM task_dependency
             WHERE task_id = 'v138-task-child' AND depends_on_id = 'v138-task-stale')",
    )
    .fetch_one(&pool)
    .await
    .expect("historical downstream rows load");
    assert_eq!(stale_history_counts, (1, 1, 1, 1, 1));

    let failed_probe_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM workspace_lease
         WHERE id IN (
            'v138-lease-extra-probe',
            'v138-lease-stale-workspace-probe',
            'v138-lease-no-primary-probe',
            'v138-lease-cross-primary-probe'
         )",
    )
    .fetch_one(&pool)
    .await
    .expect("rejected lease probes count");
    assert_eq!(failed_probe_count, 0);

    let migration_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM _migration WHERE version = 138")
            .fetch_one(&pool)
            .await
            .expect("V138 migration marker loads");
    assert_eq!(migration_count, 1);
    let foreign_key_violations: Vec<(String, i64, String, i64)> =
        sqlx::query_as("PRAGMA foreign_key_check")
            .fetch_all(&pool)
            .await
            .expect("foreign key check runs");
    assert!(foreign_key_violations.is_empty());
    let integrity: String = sqlx::query("PRAGMA integrity_check")
        .fetch_one(&pool)
        .await
        .expect("integrity check runs")
        .get(0);
    assert_eq!(integrity, "ok");

    pool.close().await;
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(migration_dir);
}
