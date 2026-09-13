use api_types::{
    ExecutionAction, ExecutionActionKind, ExecutionStatus, RecoveryAction, StateKind,
    TaskBlockingAnnotation, WorkflowDefinition,
};
use db::{AssigneeKind, ExecutionRepo, TaskRoleAssignment};
use sqlx::Row;

const INTERACTIVE_ROLE: &str = "interactive";

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
    let mut executions = Vec::with_capacity(rows.len());
    for row in rows {
        let execution_id: String = row.try_get("id")?;
        if let Some(execution) = ExecutionRepo::get_by_id(db, &execution_id).await? {
            executions.push(execution);
        }
    }
    Ok(executions)
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
                execution_status(execution) != ExecutionStatus::Running
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
                && execution_status(execution) != ExecutionStatus::Running
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

pub fn resolve_execution_actions(
    task: &db::Task,
    workflow: &WorkflowDefinition,
    executions: &[db::Execution],
    blocking_annotation: Option<&TaskBlockingAnnotation>,
    latest_review: Option<&db::Review>,
) -> Vec<ExecutionAction> {
    let is_terminal = workflow.state_kind(&task.status) == Some(StateKind::Terminal);
    let current_state = workflow
        .states
        .iter()
        .find(|state| state.name == task.status);
    let hard_failed = task.failed_json.is_some();
    let effective_role = current_state.and_then(|state| {
        state
            .role
            .as_deref()
            .or_else(|| (state.kind == StateKind::Active).then_some("assignee"))
    });

    let running_executions: Vec<&db::Execution> = executions
        .iter()
        .filter(|execution| execution_status(execution) == ExecutionStatus::Running)
        .collect();
    let has_running_execution = !running_executions.is_empty();
    let has_running_interactive_execution = running_executions
        .iter()
        .any(|execution| execution.role == INTERACTIVE_ROLE);
    let latest_non_running_execution = executions
        .iter()
        .filter(|execution| execution_status(execution) != ExecutionStatus::Running)
        .max_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });
    let latest_resumable_execution =
        latest_resumable_execution_for_role(executions, effective_role);

    let blocked_execution = blocking_annotation
        .and_then(|annotation| annotation.blocked_execution_id.as_deref())
        .and_then(|execution_id| {
            executions
                .iter()
                .find(|execution| execution.id == execution_id)
        });
    let resume_blocked_by_terminal_review = blocked_execution
        .is_some_and(|execution| terminal_review_binds_execution(execution, latest_review));
    let has_resume_recovery_action = blocking_annotation.is_some_and(|annotation| {
        annotation
            .recovery_actions
            .contains(&RecoveryAction::ResumeSession)
    });
    let has_recovery_session = has_resume_recovery_action
        && blocked_execution
            .and_then(|execution| execution.agent_session_id.as_ref())
            .is_some();
    let blocked_role_matches = effective_role
        .zip(blocked_execution.map(|execution| execution.role.as_str()))
        .is_some_and(|(role, blocked_role)| role_matches(role, blocked_role));
    let retry_budget_exhausted_reason = blocking_annotation.and_then(retry_budget_exhausted_reason);

    let re_execute_target = effective_role.and_then(|role| {
        executions
            .iter()
            .filter(|execution| {
                execution_status(execution) != ExecutionStatus::Running
                    && role_matches(role, execution.role.as_str())
            })
            .max_by(|a, b| {
                a.created_at
                    .cmp(&b.created_at)
                    .then_with(|| a.id.cmp(&b.id))
            })
    });
    let has_previous_execution_for_role = re_execute_target.is_some();
    let has_running_execution_for_role = effective_role.is_some_and(|role| {
        running_executions
            .iter()
            .any(|execution| role_matches(role, execution.role.as_str()))
    });

    vec![
        action(
            ExecutionActionKind::ManualLaunch,
            "Start Manual Execution",
            !is_terminal && !hard_failed && !has_running_interactive_execution,
            false,
            false,
            if is_terminal {
                Some("Task is in terminal state".to_owned())
            } else if hard_failed {
                Some("Task failed; reset or cancel is required".to_owned())
            } else if has_running_interactive_execution {
                Some("An execution is already running".to_owned())
            } else {
                None
            },
        ),
        action_with_target(
            ExecutionActionKind::SessionFollowUp,
            "Continue Session Manually",
            !is_terminal
                && !hard_failed
                && latest_resumable_execution.is_some()
                && !has_running_interactive_execution,
            false,
            true,
            if is_terminal {
                Some("Task is in terminal state".to_owned())
            } else if hard_failed {
                Some("Task failed; reset or cancel is required".to_owned())
            } else if has_running_interactive_execution {
                Some("An execution is already running".to_owned())
            } else if latest_resumable_execution.is_none() {
                Some(no_resumable_session_reason(effective_role))
            } else {
                None
            },
            latest_resumable_execution.map(|e| e.id.as_str()),
        ),
        action_with_target(
            ExecutionActionKind::WorkflowResume,
            format!("Resume {}", effective_role.unwrap_or("Execution")),
            !is_terminal
                && !hard_failed
                && retry_budget_exhausted_reason.is_none()
                && has_recovery_session
                && !resume_blocked_by_terminal_review
                && blocked_role_matches,
            true,
            true,
            if is_terminal {
                Some("Task is in terminal state".to_owned())
            } else if hard_failed {
                Some("Task failed; reset or cancel is required".to_owned())
            } else if retry_budget_exhausted_reason.is_some() {
                retry_budget_exhausted_reason.clone()
            } else if resume_blocked_by_terminal_review {
                Some(
                    "The bound reviewer Review attempt is terminal; start a fresh review attempt"
                        .to_owned(),
                )
            } else if !has_recovery_session {
                Some(no_resumable_session_reason(effective_role))
            } else if !blocked_role_matches {
                Some(role_mismatch_reason(
                    blocked_execution.map(|execution| execution.role.as_str()),
                    effective_role,
                ))
            } else {
                None
            },
            blocked_execution.map(|e| e.id.as_str()),
        ),
        action_with_target(
            ExecutionActionKind::ReExecute,
            format!("Re-execute {}", effective_role.unwrap_or("Execution")),
            !is_terminal
                && !hard_failed
                && retry_budget_exhausted_reason.is_none()
                && has_previous_execution_for_role
                && !has_running_execution_for_role,
            true,
            false,
            if is_terminal {
                Some("Task is in terminal state".to_owned())
            } else if hard_failed {
                Some("Task failed; reset or cancel is required".to_owned())
            } else if retry_budget_exhausted_reason.is_some() {
                retry_budget_exhausted_reason.clone()
            } else if !has_previous_execution_for_role {
                Some(re_execute_unavailable_reason(
                    latest_non_running_execution.map(|execution| execution.role.as_str()),
                    effective_role,
                ))
            } else if has_running_execution_for_role {
                Some("An execution is already running".to_owned())
            } else {
                None
            },
            re_execute_target.map(|e| e.id.as_str()),
        ),
        action(
            ExecutionActionKind::StopExecution,
            "Stop Execution",
            has_running_execution,
            false,
            false,
            if has_running_execution {
                None
            } else {
                Some("No running execution".to_owned())
            },
        ),
        action(
            ExecutionActionKind::CancelTask,
            "Cancel Task",
            !is_terminal,
            false,
            false,
            if is_terminal {
                Some("Task is already in terminal state".to_owned())
            } else {
                None
            },
        ),
    ]
}

fn terminal_review_binds_execution(execution: &db::Execution, review: Option<&db::Review>) -> bool {
    if !matches!(
        execution.role.as_str(),
        crate::workflow::default_roles::REVIEWER | crate::workflow::default_roles::AUDITOR
    ) {
        return false;
    }
    let Some(review) = review else {
        return false;
    };
    if !matches!(
        review.status,
        db::ReviewStatus::Passed | db::ReviewStatus::Failed | db::ReviewStatus::Cancelled
    ) {
        return false;
    }
    review.reviewer_execution_id.as_deref() == Some(execution.id.as_str())
        || review.auditor_execution_id.as_deref() == Some(execution.id.as_str())
        || (review.reviewer_execution_id.is_none()
            && review.auditor_execution_id.is_none()
            && review.execution_id == execution.id)
}

fn no_resumable_session_reason(role: Option<&str>) -> String {
    format!(
        "No resumable {} session available",
        role.unwrap_or("execution")
    )
}

fn role_mismatch_reason(other_role: Option<&str>, current_role: Option<&str>) -> String {
    format!(
        "Latest execution is for {}, not {}",
        other_role.unwrap_or("unknown"),
        current_role.unwrap_or("current role")
    )
}

fn re_execute_unavailable_reason(
    latest_role: Option<&str>,
    effective_role: Option<&str>,
) -> String {
    match (latest_role, effective_role) {
        (Some(other_role), Some(current_role)) if other_role != current_role => {
            role_mismatch_reason(Some(other_role), Some(current_role))
        }
        _ => format!(
            "No previous {} execution available",
            effective_role.unwrap_or("role")
        ),
    }
}

fn retry_budget_exhausted_reason(annotation: &TaskBlockingAnnotation) -> Option<String> {
    // Annotations here may be synthesized from blocked metadata (see
    // blocked_metadata_annotation in the api layer), so both exhaustion
    // vocabularies apply.
    let exhausted = annotation.annotation_type.is_budget_exhausted_annotation()
        || annotation.annotation_type.is_retry_exhausted_metadata();
    exhausted.then(|| {
        format!(
            "Retry budget exhausted for {}",
            retry_budget_gate(annotation)
        )
    })
}

fn retry_budget_gate(annotation: &TaskBlockingAnnotation) -> String {
    match annotation.annotation_type {
        api_types::FailureKind::ReviewBudgetExhausted => "review".to_owned(),
        api_types::FailureKind::MergeFixBudgetExhausted => "merge_fix".to_owned(),
        _ => annotation.blocking_reason.clone(),
    }
}

fn action(
    action: ExecutionActionKind,
    label: impl Into<String>,
    enabled: bool,
    propagates: bool,
    requires_session: bool,
    disabled_reason: Option<String>,
) -> ExecutionAction {
    action_with_target(
        action,
        label,
        enabled,
        propagates,
        requires_session,
        disabled_reason,
        None,
    )
}

fn action_with_target(
    action: ExecutionActionKind,
    label: impl Into<String>,
    enabled: bool,
    propagates: bool,
    requires_session: bool,
    disabled_reason: Option<String>,
    target_execution_id: Option<&str>,
) -> ExecutionAction {
    ExecutionAction {
        action,
        label: label.into(),
        enabled,
        propagates,
        requires_session,
        disabled_reason,
        target_execution_id: target_execution_id.map(str::to_owned),
    }
}

fn execution_status(execution: &db::Execution) -> ExecutionStatus {
    match execution.status {
        db::ExecutionStatus::Running => ExecutionStatus::Running,
        db::ExecutionStatus::Completed => ExecutionStatus::Completed,
        db::ExecutionStatus::Failed => ExecutionStatus::Failed,
        db::ExecutionStatus::Cancelled => ExecutionStatus::Cancelled,
    }
}
