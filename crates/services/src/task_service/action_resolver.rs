use db::{AssigneeKind, ExecutionRepo, TaskRoleAssignment};
use sqlx::Row;


fn role_matches(role: &str, execution_role: &str) -> bool {
    role == execution_role
        || (role == crate::workflow::default_roles::CODER && execution_role == "executor")
}

/// The latest transition-log entry into a role-owned state is one of the
/// inputs used to decide whether a completed execution belongs to the
/// current pass. Keep only the identity and ordering fields that action
/// authority observes; hook payload changes do not alter that decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatestStateEntryAuthority {
    pub id: String,
    pub created_at: String,
}

/// Return the newest transition into `to_state` without loading the task's
/// complete transition history. This mirrors `available_task_actions_for`'s
/// `latest_state_entry` projection and is also used by the `/actions`
/// coherence fingerprint.
pub async fn latest_state_entry_authority(
    db: &db::SqliteDb,
    task_id: &str,
    to_state: &str,
) -> crate::Result<Option<LatestStateEntryAuthority>> {
    let row = sqlx::query(
        "SELECT id, created_at FROM transition_log WHERE task_id = ? AND to_state = ? ORDER BY created_at DESC, rowid DESC LIMIT 1",
    )
    .bind(task_id)
    .bind(to_state)
    .fetch_optional(db.pool())
    .await?;

    row.map(|row| {
        Ok(LatestStateEntryAuthority {
            id: row.try_get("id")?,
            created_at: row.try_get("created_at")?,
        })
    })
    .transpose()
}

/// Load the bounded execution projection needed by action authority.
///
/// The resolver does not need the complete history. It needs the latest row
/// in each authority class: latest non-running, latest resumable, latest
/// agent-backed, latest running, latest interactive-running, and the latest
/// current-role row in each of those classes, plus the explicitly blocked
/// execution. Selecting those representatives in SQL avoids both the old
/// newest-100 truncation and an unbounded per-task history scan. The
/// coder/executor alias is kept here because older executions used `executor`
/// for the coder role.
pub async fn list_execution_action_authority(
    db: &db::SqliteDb,
    task_id: &str,
    current_role: Option<&str>,
    blocked_execution_id: Option<&str>,
) -> crate::Result<Vec<db::Execution>> {
    let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new("SELECT DISTINCT id FROM (");
    let mut first = true;

    macro_rules! candidate_static {
        ($predicate:expr) => {{
            if !first {
                query.push(" UNION ALL ");
            }
            first = false;
            query
                .push("SELECT id FROM (SELECT id FROM execution WHERE task_id = ")
                .push_bind(task_id)
                .push(" AND ")
                .push($predicate)
                .push(" ORDER BY created_at DESC, id DESC LIMIT 1)");
        }};
    }

    macro_rules! candidate_role {
        ($predicate:expr, $role:expr) => {{
            if !first {
                query.push(" UNION ALL ");
            }
            first = false;
            query
                .push("SELECT id FROM (SELECT id FROM execution WHERE task_id = ")
                .push_bind(task_id)
                .push(" AND ")
                .push($predicate)
                .push_bind($role)
                .push(" ORDER BY created_at DESC, id DESC LIMIT 1)");
        }};
    }

    macro_rules! candidate_coder_roles {
        ($predicate:expr, $role:expr) => {{
            if !first {
                query.push(" UNION ALL ");
            }
            first = false;
            query
                .push("SELECT id FROM (SELECT id FROM execution WHERE task_id = ")
                .push_bind(task_id)
                .push(" AND ")
                .push($predicate)
                .push_bind($role)
                .push(", 'executor') ORDER BY created_at DESC, id DESC LIMIT 1)");
        }};
    }

    macro_rules! candidate_id {
        ($execution_id:expr) => {{
            if !first {
                query.push(" UNION ALL ");
            }
            query
                .push("SELECT id FROM (SELECT id FROM execution WHERE task_id = ")
                .push_bind(task_id)
                .push(" AND id = ")
                .push_bind($execution_id)
                .push(" ORDER BY created_at DESC, id DESC LIMIT 1)");
        }};
    }

    candidate_static!("status != 'running'");
    candidate_static!("status != 'running' AND agent_session_id IS NOT NULL");
    // `action_agent_id` can fall back to the newest execution with an agent
    // identity when there is no role assignment. It includes running rows;
    // preserve that authority class in the bounded projection as well.
    // `action_agent_id` falls back to the newest agent-backed execution even
    // when that row is still running. Keep that exact authority source in the
    // bounded projection so the `/actions` fingerprint observes the same row
    // the resolver can use.
    candidate_static!("agent_id IS NOT NULL");
    candidate_static!("status = 'running'");
    candidate_static!("status = 'running' AND role = 'interactive'");
    if let Some(role) = current_role {
        if role == crate::workflow::default_roles::CODER {
            candidate_coder_roles!("status != 'running' AND role IN (", role);
            candidate_coder_roles!(
                "status != 'running' AND agent_session_id IS NOT NULL AND role IN (",
                role
            );
            candidate_coder_roles!(
                "status != 'running' AND agent_id IS NOT NULL AND role IN (",
                role
            );
            candidate_coder_roles!("status = 'running' AND role IN (", role);
        } else {
            candidate_role!("status != 'running' AND role = ", role);
            candidate_role!(
                "status != 'running' AND agent_session_id IS NOT NULL AND role = ",
                role
            );
            candidate_role!(
                "status != 'running' AND agent_id IS NOT NULL AND role = ",
                role
            );
            candidate_role!("status = 'running' AND role = ", role);
        }
    }
    if let Some(execution_id) = blocked_execution_id {
        candidate_id!(execution_id);
    }

    // The resolver consumes this as a set, but the `/actions` coherence
    // fingerprint compares the projected rows directly. Make the projection
    // order deterministic so an unchanged authority snapshot cannot look
    // different merely because SQLite chose another UNION scan order.
    query.push(") candidates ORDER BY id");
    let rows = query.build().fetch_all(db.pool()).await?;
    let execution_ids = rows
        .iter()
        .map(|row| row.try_get::<String, _>("id"))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let execution_ids = execution_ids.iter().map(String::as_str).collect::<Vec<_>>();
    Ok(ExecutionRepo::get_by_ids(db, &execution_ids).await?)
}

/// Select the newest resumable execution for the Task's effective role.
///
/// This is the in-memory counterpart to the role-aware SQL representatives
/// loaded by [`list_execution_action_authority`]. Keep the coder/executor
/// alias in the same predicate used by execution actions so a historical
/// executor row remains a valid coder-session target.
pub fn latest_resumable_execution_for_role<'a>(
    executions: &'a [db::Execution],
    effective_role: Option<&str>,
) -> Option<&'a db::Execution> {
    effective_role.and_then(|role| {
        executions
            .iter()
            .filter(|execution| {
                execution.status != db::ExecutionStatus::Running
                    && execution.agent_session_id.is_some()
                    && role_matches(role, execution.role.as_str())
            })
            .max_by(|left, right| {
                left.created_at
                    .cmp(&right.created_at)
                    .then_with(|| left.id.cmp(&right.id))
            })
    })
}

/// Select the execution that recovery's Open Interactive follow-up may use.
/// An explicitly blocked resumable execution wins; otherwise use the latest
/// resumable execution for the current role. Unrelated newer reviewer or
/// auditor rows are deliberately ignored.
pub fn select_open_interactive_target<'a>(
    executions: &'a [db::Execution],
    effective_role: Option<&str>,
    blocked_execution_id: Option<&str>,
) -> Option<&'a db::Execution> {
    if let (Some(role), Some(blocked_execution_id)) = (effective_role, blocked_execution_id) {
        if let Some(execution) = executions.iter().find(|execution| {
            execution.id == blocked_execution_id
                && execution.status != db::ExecutionStatus::Running
                && execution.agent_session_id.is_some()
                && role_matches(role, execution.role.as_str())
        }) {
            return Some(execution);
        }
    }

    latest_resumable_execution_for_role(executions, effective_role)
}

/// Mirror `TaskService::interactive_launch_agent`'s fresh-launch fallbacks
/// using the same bounded execution authority: an assigned current-role agent,
/// the explicitly blocked execution's agent, or the newest agent-backed
/// execution (represented by the static agent-backed candidate).
pub fn has_open_interactive_launch_authority(
    executions: &[db::Execution],
    role_assignments: &[TaskRoleAssignment],
    effective_role: Option<&str>,
    blocked_execution_id: Option<&str>,
) -> bool {
    let assigned_agent = effective_role.is_some_and(|role| {
        role_assignments.iter().any(|assignment| {
            assignment.role_name == role
                && assignment.assignee_type == Some(AssigneeKind::Agent)
                && assignment.assignee_id.is_some()
        })
    });
    let blocked_agent = blocked_execution_id.is_some_and(|execution_id| {
        executions
            .iter()
            .any(|execution| execution.id == execution_id && execution.agent_id.is_some())
    });
    let previous_agent = executions
        .iter()
        .any(|execution| execution.agent_id.is_some());

    assigned_agent || blocked_agent || previous_agent
}

