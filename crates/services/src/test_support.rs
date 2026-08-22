use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

use db::{now_rfc3339, SqliteDb};
use serde_json::json;

static AFTER_DOMAIN_COMMIT: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

fn armed() -> &'static Mutex<HashSet<String>> {
    AFTER_DOMAIN_COMMIT.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Arm a one-shot failpoint for the action's post-domain/pre-receipt seam.
///
/// This module is only compiled for the services crate's unit-test build.  A
/// one-shot action id keeps tests independent while still allowing the normal
/// service execution path to be exercised end to end.
pub(crate) fn arm_after_domain_commit(action_id: &str) {
    armed()
        .lock()
        .expect("characterization failpoint lock")
        .insert(action_id.to_owned());
}

pub(crate) fn take_after_domain_commit(action_id: &str) -> bool {
    armed()
        .lock()
        .expect("characterization failpoint lock")
        .remove(action_id)
}

/// Make a legacy Project fixture explicit about the repository execution
/// roles introduced by V087.  These tests intentionally keep the legacy
/// Charter state, but their repository execution paths still cross the same
/// role/lease admission boundary as a Charter-backed Project.
pub(crate) async fn configure_project_execution_test_setup(
    db: &SqliteDb,
    project_id: &str,
    worker_id: &str,
    reviewer_id: &str,
) {
    let now = now_rfc3339();
    let existing_settings: Option<String> =
        sqlx::query_scalar("SELECT settings FROM project WHERE id = ?")
            .bind(project_id)
            .fetch_optional(db.pool())
            .await
            .expect("test project settings lookup");
    let mut settings = existing_settings
        .as_deref()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .filter(serde_json::Value::is_object)
        .unwrap_or_else(|| json!({}));
    settings["default_role_assignments"] = json!([
        {
            "role_name": "planner",
            "assignee_type": "agent",
            "assignee_id": worker_id
        },
        {
            "role_name": "coder",
            "assignee_type": "agent",
            "assignee_id": worker_id
        },
        {
            "role_name": "reviewer",
            "assignee_type": "agent",
            "assignee_id": reviewer_id
        }
    ]);
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(settings.to_string())
        .bind(&now)
        .bind(project_id)
        .execute(db.pool())
        .await
        .expect("test project execution role settings update");

    let operation_id: String =
        sqlx::query_scalar("SELECT id FROM project_provisioning_operation WHERE project_id = ?")
            .bind(project_id)
            .fetch_one(db.pool())
            .await
            .expect("test project provisioning operation lookup");
    sqlx::query(
        "UPDATE project_provisioning_operation
         SET status = 'ready', current_checkpoint = 'completed',
             lease_owner = NULL, lease_expires_at = NULL, next_retry_at = NULL,
             retryable = 0, completed_at = ?, updated_at = ?, version = version + 1
         WHERE id = ? AND project_id = ?",
    )
    .bind(&now)
    .bind(&now)
    .bind(&operation_id)
    .bind(project_id)
    .execute(db.pool())
    .await
    .expect("test project provisioning operation ready update");
    for checkpoint in [
        "preflight",
        "repository_initialized",
        "repository_registered",
        "repository_linked",
        "roles_assigned",
    ] {
        sqlx::query(
            "UPDATE project_provisioning_checkpoint
             SET status = 'completed', completed_at = ?, updated_at = ?,
                 version = version + 1
             WHERE operation_id = ? AND checkpoint = ?",
        )
        .bind(&now)
        .bind(&now)
        .bind(&operation_id)
        .bind(checkpoint)
        .execute(db.pool())
        .await
        .expect("test project provisioning checkpoint ready update");
    }
}

/// Opt an interactive-only service fixture out of the project default-role
/// dispatch that would otherwise run as an on-enter side effect while the
/// launch API moves a task from its initial state into active work.
pub(crate) async fn clear_project_execution_role_defaults(db: &SqliteDb, project_id: &str) {
    let existing_settings: Option<String> =
        sqlx::query_scalar("SELECT settings FROM project WHERE id = ?")
            .bind(project_id)
            .fetch_optional(db.pool())
            .await
            .expect("test project settings lookup");
    let mut settings = existing_settings
        .as_deref()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .filter(serde_json::Value::is_object)
        .unwrap_or_else(|| json!({}));
    settings
        .as_object_mut()
        .expect("project settings object")
        .remove("default_role_assignments");
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(settings.to_string())
        .bind(now_rfc3339())
        .bind(project_id)
        .execute(db.pool())
        .await
        .expect("test project execution role settings clear");
}

/// Restore a fixture agent's capacity after a normal execution fixture has
/// been seeded.  The normal helpers reserve one slot because lease admission
/// revalidates the identity after the new execution row is visible; tests
/// whose purpose is capacity behavior opt back into the exact one-slot limit.
pub(crate) async fn set_test_agent_capacity(db: &SqliteDb, agent_id: &str, capacity: i64) {
    sqlx::query("UPDATE agent_identity SET max_concurrent_tasks = ?, updated_at = ? WHERE id = ?")
        .bind(capacity)
        .bind(now_rfc3339())
        .bind(agent_id)
        .execute(db.pool())
        .await
        .expect("test agent capacity update");
}

/// Make a lower-priority candidate lose the scheduler's optimistic version
/// race after the higher-priority candidate has been dispatched.  The
/// dispatcher snapshots all initial candidates before transitioning them, so
/// this leaves the lower-priority Task in its original state while keeping
/// the first dispatch's asynchronous execution lease untouched.
pub(crate) async fn force_task_version_conflict_after_transition(
    db: &SqliteDb,
    first_task_id: &str,
    to_state: &str,
    competing_task_id: &str,
) {
    let trigger_name = format!(
        "test_priority_version_conflict_{}",
        first_task_id.replace(['-', '.'], "_")
    );
    let first_task_id = first_task_id.replace('\'', "''");
    let to_state = to_state.replace('\'', "''");
    let competing_task_id = competing_task_id.replace('\'', "''");
    let now = now_rfc3339().replace('\'', "''");
    let sql = format!(
        "CREATE TRIGGER \"{trigger_name}\"
         AFTER UPDATE OF hook_results_json ON transition_log
         WHEN NEW.task_id = '{first_task_id}'
          AND NEW.to_state = '{to_state}'
          AND NEW.hook_results_json IS NOT NULL
         BEGIN
             UPDATE task
             SET version = version + 1, updated_at = '{now}'
             WHERE id = '{competing_task_id}';
         END"
    );
    sqlx::query(&sql)
        .execute(db.pool())
        .await
        .expect("test agent capacity cutover trigger creates");
}
