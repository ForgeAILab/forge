use db::ExecutionRepo;

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
    let row = db.latest_task_state_entry_source(task_id, to_state).await?;

    row.map(|row| {
        Ok(LatestStateEntryAuthority {
            id: row.id()?,
            created_at: row.created_at()?,
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
    let execution_ids = db
        .execution_action_authority_ids(
            task_id,
            current_role,
            blocked_execution_id,
            crate::workflow::default_roles::CODER,
        )
        .await?;
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
