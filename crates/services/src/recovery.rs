use crate::{
    daemon_transport::DaemonConnectionRegistry, embedded_daemon::is_embedded_daemon_machine,
    workflow::engine::WorkflowEngine, workspace_backend::EmbeddedWorkspaceBackend, Result,
    ServiceError, TaskService,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
#[cfg(test)]
use db::UpdateExecution;
use db::{
    now_rfc3339, Agent, AgentListQuery, AgentRepo, AgentSessionRepo, AgentStatus, Daemon,
    DaemonRepo, Execution, ExecutionLeaseDisposition, ExecutionProgressWarningOutcome,
    ExecutionRepo, ExecutionStatus, ExecutionTerminalOutcome, MarkUsageInvocationUnsettled,
    PageRequest, PlacementFailureCause, PlacementOwnerKind, PlacementState, Project, ProjectRepo,
    RecordExecutionProgressWarning, ResumePolicy, SortBy, SortOrder, SqliteDb, StopReason, Task,
    TaskListQuery, TaskRepo, TerminalizeExecution, UpdateAgent, UpdateTaskStatus,
    UpdateWorkspacePlacement, UsageLedgerRepo, WorkspaceLeaseRepo, WorkspacePlacement,
    WorkspacePlacementRepo, WorkspaceRepo,
};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent};
use executors::TaskExecutor;
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tracing::Instrument;

pub(crate) async fn resolve_execution_daemon(
    db: &SqliteDb,
    execution: &Execution,
) -> Result<Option<(String, Daemon)>> {
    let daemon_id = if let Some(workspace_id) = execution.workspace_id.as_deref() {
        let placement = match db::WorkspacePlacementRepo::get_by_workspace_id(db, workspace_id)
            .await?
        {
            Some(placement) => placement,
            None => {
                let workspace = db::WorkspaceRepo::get_by_id(db, workspace_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("workspace", workspace_id.to_owned()))?;
                EmbeddedWorkspaceBackend::ensure_recorded_server_placement(db, &workspace).await?
            }
        };
        match placement.owner_kind {
            PlacementOwnerKind::Daemon => placement.daemon_id,
            PlacementOwnerKind::Server => placement.execution_daemon_id,
        }
    } else {
        // No workspace means no placement can exist. Preserve task-0 routing
        // for these executions; placed work never falls back to an Agent pin.
        let snapshot = execution
            .executor_config_snapshot_json
            .as_deref()
            .map(serde_json::from_str::<serde_json::Value>)
            .transpose()
            .map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "invalid executor config snapshot: {error}"
                ))
            })?;
        match snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.get("daemon_id"))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|id| !id.is_empty())
        {
            Some(id) => Some(id.to_owned()),
            None => match execution.agent_id.as_deref() {
                Some(id) => AgentRepo::get_by_id(db, id)
                    .await?
                    .and_then(|agent| agent.daemon_id),
                None => None,
            },
        }
    };
    let Some(daemon_id) = daemon_id else {
        return Ok(None);
    };
    let Some(daemon) = DaemonRepo::get_by_id(db, &daemon_id).await? else {
        return Ok(None);
    };
    Ok(Some((daemon_id, daemon)))
}

pub(crate) const DAEMON_REPORT_RECONCILE_MIN_AGE: Duration = Duration::from_secs(60);

pub(crate) struct RemovedMachineSettlement {
    /// The Task lost a run or its workspace and must be admitted again.
    pub lost_work: bool,
    pub role_events: Vec<crate::task_service::RoleSweepEvent>,
}

pub(crate) const MACHINE_REMOVED_REASON: &str = "machine_removed";
async fn discard_execution_plan_artifacts(db: &SqliteDb, execution: &Execution) {
    let Some(workspace_id) = execution.workspace_id.as_deref() else {
        return;
    };
    let workspace = match WorkspaceRepo::get_by_id(db, workspace_id).await {
        Ok(Some(workspace)) => workspace,
        Ok(None) => return,
        Err(error) => {
            tracing::warn!(
                execution_id = %execution.id,
                %error,
                "failed to resolve workspace while cleaning terminal plan artifacts"
            );
            return;
        }
    };
    // The outbox and plan stage sit in the Task root, beside the worktree,
    // and must go even when the worktree itself is no longer there.
    let Ok(placement) =
        EmbeddedWorkspaceBackend::ensure_recorded_server_placement(db, &workspace).await
    else {
        return;
    };
    let Ok(path) = crate::workspace_manager::task_root_anchor_of(&placement) else {
        return;
    };
    let worktree = path.as_path();
    // A run that died with the process never removed its temp directory.
    executors::sandbox::settle_run(worktree, &execution.id);
    if let Some(outbox) = executors::execution_outbox_path(worktree, &execution.id) {
        if let Err(error) = std::fs::remove_dir_all(&outbox) {
            if error.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    execution_id = %execution.id,
                    path = %outbox.display(),
                    %error,
                    "failed to remove terminal execution outbox"
                );
            }
        }
    }
    crate::task_service::execution::discard_execution_plan_stage(
        &path.to_string_lossy(),
        &execution.id,
    );
}

#[derive(Clone)]
pub struct CrashRecovery {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
}

pub struct HeartbeatMonitor {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
    task_service: Option<Arc<TaskService>>,
    task_executor: Option<Arc<dyn TaskExecutor>>,
    daemon_connections: Option<Arc<DaemonConnectionRegistry>>,
    check_interval: Duration,
    execution_stall_timeout: Duration,
    max_disconnect: Duration,
    placement_in_flight: Arc<Mutex<HashSet<String>>>,
    placement_workers: Arc<tokio::sync::Semaphore>,
    placement_worker_handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    stopped: AtomicBool,
    stop_notify: tokio::sync::Notify,
}

/// How long a freshly created `dispatch-pending:` lease marker is left alone
/// before recovery may reclaim it. The local runner claims its lease within
/// milliseconds; this only has to outlast that handoff.
const DISPATCH_PENDING_GRACE_SECONDS: i64 = 30;

/// Prefix of the placeholder lease owner installed with a locally dispatched
/// execution, before its runner takes ownership.
const DISPATCH_PENDING_OWNER_PREFIX: &str = "dispatch-pending:";

/// Whether this expired-lease row is a dispatch marker still inside its grace
/// window, and so not yet evidence of a dead owner. A row past its hard
/// deadline is always eligible, marker or not.
fn is_unclaimed_dispatch_marker(execution: &Execution, now: &str) -> bool {
    let Some(owner) = execution.lease_owner.as_deref() else {
        return false;
    };
    if !owner.starts_with(DISPATCH_PENDING_OWNER_PREFIX) {
        return false;
    }
    if execution
        .hard_deadline_at
        .as_deref()
        .is_some_and(|deadline| deadline <= now)
    {
        return false;
    }
    let (Ok(created_at), Ok(now)) = (
        DateTime::parse_from_rfc3339(&execution.created_at),
        DateTime::parse_from_rfc3339(now),
    ) else {
        return false;
    };
    now.signed_duration_since(created_at) < ChronoDuration::seconds(DISPATCH_PENDING_GRACE_SECONDS)
}

pub(crate) struct CancelledExecution {
    pub execution_id: String,
    pub agent_session_id: Option<String>,
}

pub(crate) struct RecoverTaskOutcome {
    pub task: Task,
    pub annotated: bool,
    /// Running executions this recovery settled.
    pub settled_executions: usize,
}

fn execution_owner_lease_is_healthy(execution: &Execution, now: &str) -> bool {
    if execution.status != ExecutionStatus::Running || execution.lease_owner.is_none() {
        return false;
    }
    let Some(lease_expires_at) = execution.lease_expires_at.as_deref() else {
        return false;
    };
    if !rfc3339_is_after(lease_expires_at, now) {
        return false;
    }
    execution
        .hard_deadline_at
        .as_deref()
        .is_none_or(|hard_deadline_at| rfc3339_is_after(hard_deadline_at, now))
}

fn rfc3339_is_after(value: &str, other: &str) -> bool {
    let Some(value) = DateTime::parse_from_rfc3339(value).ok() else {
        return false;
    };
    let Some(other) = DateTime::parse_from_rfc3339(other).ok() else {
        return false;
    };
    value > other
}

async fn list_projects(db: &SqliteDb) -> Result<Vec<Project>> {
    let mut projects = Vec::new();
    let mut cursor = None;
    loop {
        let page = ProjectRepo::list(db, page_request(cursor)).await?;
        projects.extend(page.items);
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    Ok(projects)
}

fn page_request(cursor: Option<String>) -> PageRequest {
    PageRequest {
        cursor,
        limit: 500,
        include_total: false,
        sort_by: SortBy::CreatedAt,
        sort_order: SortOrder::Asc,
    }
}

fn stalled_execution_should_block_task(execution: &db::Execution) -> bool {
    matches!(
        execution.role.as_str(),
        "interactive" | "executor" | crate::workflow::default_roles::CODER
    )
}

/// Reviewers have their own settlement/retry state machine.  Any monitor that
/// wins a terminal execution CAS must enter that cascade; sending a reviewer
/// through the generic worker blocker leaves its `review` row running forever.
async fn cascade_recovered_execution(
    task_service: &TaskService,
    execution: &Execution,
    block_non_reviewer: bool,
) -> Result<()> {
    if crate::task_service::retired_review_rerun(execution) {
        return task_service.settle_retired_review_rerun(execution).await;
    }
    if execution.role == crate::workflow::default_roles::REVIEWER {
        task_service
            .maybe_cascade_executor_completion(&execution.id)
            .await
    } else if block_non_reviewer {
        task_service
            .annotate_executor_failure_block(execution)
            .await
    } else {
        Ok(())
    }
}

pub(crate) struct FailDaemonDisconnectedExecution<'a> {
    pub execution: &'a Execution,
    pub daemon_id: &'a str,
    pub error_message: String,
    pub stopped_by: &'a str,
    pub reconciliation_reason: &'a str,
}

fn placement_update(
    placement: &WorkspacePlacement,
    state: PlacementState,
    failure_cause: Option<PlacementFailureCause>,
) -> UpdateWorkspacePlacement {
    UpdateWorkspacePlacement {
        id: placement.id.clone(),
        expected_version: placement.version,
        agent_id: None,
        owner_kind: None,
        daemon_id: None,
        runtime_id: None,
        repo_location_id: None,
        execution_daemon_id: None,
        workspace_handle: None,
        generation: None,
        state: Some(state),
        selected_by: None,
        selection_reason: None,
        reserved_until: None,
        disconnected_at: None,
        failure_cause: Some(failure_cause),
        updated_at: now_rfc3339(),
    }
}

pub(crate) fn placement_execution_daemon_id(placement: &WorkspacePlacement) -> Option<&str> {
    match placement.owner_kind {
        PlacementOwnerKind::Daemon => placement.daemon_id.as_deref(),
        PlacementOwnerKind::Server => placement.execution_daemon_id.as_deref(),
    }
}

// An attempt owns its daemon and each placement until completion or cancellation.
struct PlacementAttemptGuard {
    in_flight: Arc<Mutex<HashSet<String>>>,
    keys: Vec<String>,
}
impl Drop for PlacementAttemptGuard {
    fn drop(&mut self) {
        let mut in_flight = self
            .in_flight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for key in &self.keys {
            in_flight.remove(key);
        }
    }
}

fn agent_timed_out(agent: &Agent) -> bool {
    let Some(last_heartbeat_at) = &agent.last_heartbeat_at else {
        return true;
    };
    let Some(last_heartbeat_at) = parse_rfc3339_unix_seconds(last_heartbeat_at) else {
        return true;
    };
    let heartbeat_interval = agent.heartbeat_interval_seconds.max(1) as u64;
    let max_missed = agent.max_missed_heartbeats.max(1) as u64;
    let timeout = heartbeat_interval.saturating_mul(max_missed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);

    now.saturating_sub(last_heartbeat_at) > timeout
}

fn parse_rfc3339_unix_seconds(value: &str) -> Option<u64> {
    let (date, time_with_offset) = value.split_once('T')?;
    let mut date_parts = date.split('-');
    let year = date_parts.next()?.parse::<i32>().ok()?;
    let month = date_parts.next()?.parse::<u32>().ok()?;
    let day = date_parts.next()?.parse::<u32>().ok()?;

    let (time, offset_seconds) = split_time_offset(time_with_offset)?;
    let mut time_parts = time.split(':');
    let hour = time_parts.next()?.parse::<u32>().ok()?;
    let minute = time_parts.next()?.parse::<u32>().ok()?;
    let second = time_parts.next()?.split('.').next()?.parse::<u32>().ok()?;

    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }

    let days = days_from_civil(year, month, day);
    let seconds = days
        .checked_mul(86_400)?
        .checked_add((hour as i64).checked_mul(3_600)?)?
        .checked_add((minute as i64).checked_mul(60)?)?
        .checked_add(second as i64)?
        .checked_sub(offset_seconds as i64)?;

    u64::try_from(seconds).ok()
}

fn split_time_offset(value: &str) -> Option<(&str, i32)> {
    if let Some(time) = value.strip_suffix('Z') {
        return Some((time, 0));
    }
    let offset_index = value
        .char_indices()
        .skip(1)
        .find_map(|(index, character)| matches!(character, '+' | '-').then_some(index))?;
    let (time, offset) = value.split_at(offset_index);
    let sign = if offset.starts_with('+') { 1 } else { -1 };
    let mut offset_parts = offset[1..].split(':');
    let hours = offset_parts.next()?.parse::<i32>().ok()?;
    let minutes = offset_parts.next()?.parse::<i32>().ok()?;
    if hours > 23 || minutes > 59 {
        return None;
    }
    Some((time, sign * (hours * 3_600 + minutes * 60)))
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month as i32;
    let day = day as i32;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146_097 + doe - 719_468).into()
}

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

mod crash;

mod heartbeat;

mod workspace_leases;
use workspace_leases::{expire_workspace_leases, renew_workspace_leases};

mod tasks;
pub(crate) use tasks::{cancel_running_executions, recover_task, stop_retired_review_reruns};
use tasks::{
    list_in_progress_tasks, sweep_stale_recovery_annotations, tasks_with_running_executions,
};

mod execution_owner;
use execution_owner::record_disconnected_attention;
pub(crate) use execution_owner::{
    disconnect_daemon_placements, execution_is_remote_owned, execution_lease_is_suspended,
    execution_placement_is_disconnected, fail_execution_daemon_disconnected,
    reconcile_daemon_report_executions, suspend_expired_remote_execution,
};

mod placement_monitor;

mod owner_failure;
use owner_failure::{
    annotate_owner_workspace_reset, fail_owned_execution, fail_placement_executions,
    reconcile_owner_receipts, repair_owner_recovery, terminalize_owner_lost,
};
pub(crate) use owner_failure::{apply_owner_cleanup, owner_execution_failure_cause};

mod reconciliation;
#[cfg(test)]
use reconciliation::complete_workspace_reconciliation;
use reconciliation::finish_reconciled_cascades;
pub(crate) use reconciliation::reconcile_workspace_placement;

mod removed_machine;
pub(crate) use removed_machine::{settle_removed_machine_task, RemovedMachineFacts};

#[cfg(test)]
pub(crate) mod tests;

#[cfg(test)]
mod removed_machine_tests;
