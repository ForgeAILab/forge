//! Public reads of the merge queue: where a Task is in line and why it waits.
//!
//! Read-only. The queue rows are the queue worker's, the Task condition is
//! the integration steps'; this module only projects both into the one
//! summary REST, MCP, `forge-ctl` and the web read. It takes no write
//! transaction and changes no row.
use crate::{
    integration_steps::gate::task_row_gate,
    integration_worker::{GateCause, ParkReason},
    Result,
};
use api_types::{
    IntegrationActionHint, IntegrationDeferralCause, IntegrationMemberState, IntegrationPhase,
    IntegrationQueueMemberResponse, IntegrationQueueResponse, IntegrationQueueState,
    IntegrationQueueStateReason, IntegrationQueuesResponse, IntegrationReason, IntegrationTimings,
    IntegrationWait, IntegrationWaitCode, TaskIntegrationSummary,
};
use db::{
    IntegrationAttemptState as S, IntegrationCheckTiming, IntegrationFailureKind,
    IntegrationMemberRead, SqliteDb,
};
use std::collections::HashMap;

fn queue_state(state: db::IntegrationQueueState) -> IntegrationQueueState {
    match state {
        db::IntegrationQueueState::Open => IntegrationQueueState::Open,
        db::IntegrationQueueState::Suspended => IntegrationQueueState::Suspended,
        db::IntegrationQueueState::Quarantined => IntegrationQueueState::Quarantined,
        db::IntegrationQueueState::Closed => IntegrationQueueState::Closed,
    }
}

fn gate_code(cause: GateCause) -> IntegrationWaitCode {
    match cause {
        GateCause::Held => IntegrationWaitCode::TaskHeld,
        GateCause::Blocked => IntegrationWaitCode::TaskBlocked,
        GateCause::Failed => IntegrationWaitCode::TaskFailed,
        GateCause::EntryBlocked => IntegrationWaitCode::TaskEntryBlocked,
        GateCause::AwaitingHuman => IntegrationWaitCode::TaskAwaitingHuman,
        GateCause::ProjectPaused => IntegrationWaitCode::ProjectPaused,
    }
}

const GATES: [GateCause; 6] = [
    GateCause::Held,
    GateCause::Blocked,
    GateCause::Failed,
    GateCause::EntryBlocked,
    GateCause::AwaitingHuman,
    GateCause::ProjectPaused,
];

/// The worker's park reason an attempt's `failure_message` leads with
/// (`<code>: <detail>`), and the detail.
fn park_reason(message: &str) -> (Option<ParkReason>, &str) {
    let (code, detail) = message
        .split_once(':')
        .map_or((message, ""), |(code, detail)| (code, detail.trim()));
    let reason = GATES
        .into_iter()
        .map(ParkReason::Gate)
        .chain([
            ParkReason::Infrastructure,
            ParkReason::Timeout,
            ParkReason::TargetNotReady,
            ParkReason::TargetDirty,
            ParkReason::WorktreeDirty,
            ParkReason::UnsupportedConflict,
            ParkReason::TransferTooLarge,
            ParkReason::ExternalMovesExhausted,
            ParkReason::RoundsExhausted,
            ParkReason::StepFailed,
        ])
        .find(|reason| reason.code() == code);
    match reason {
        Some(reason) => (Some(reason), detail),
        None => (None, message),
    }
}

fn deferral_code(cause: &IntegrationDeferralCause) -> IntegrationWaitCode {
    match cause {
        IntegrationDeferralCause::Infrastructure => IntegrationWaitCode::Infrastructure,
        IntegrationDeferralCause::OwnerOffline => IntegrationWaitCode::OwnerOffline,
        IntegrationDeferralCause::TargetDirty => IntegrationWaitCode::TargetDirty,
        IntegrationDeferralCause::BudgetExhausted => IntegrationWaitCode::BudgetExhausted,
        IntegrationDeferralCause::OwnerRequired => IntegrationWaitCode::OwnerRequired,
        IntegrationDeferralCause::UnresolvedResult => IntegrationWaitCode::UnresolvedResult,
    }
}

fn on_a_machine(code: IntegrationWaitCode) -> bool {
    matches!(
        code,
        IntegrationWaitCode::OwnerOffline
            | IntegrationWaitCode::UnresolvedResult
            | IntegrationWaitCode::QueueSuspended
            | IntegrationWaitCode::QueueQuarantined
    )
}

fn non_empty(text: &str) -> Option<String> {
    (!text.trim().is_empty()).then(|| text.trim().to_owned())
}

/// The summary of one attempt in progress. `task` is the Task it belongs to
/// when the caller has it: its own row states a Hold or a block, and its
/// condition names what the integration steps told it. `None` for an attempt
/// that has left the queue.
pub fn task_integration_summary(
    row: &IntegrationMemberRead,
    task: Option<&db::Task>,
    project_paused: bool,
) -> Option<TaskIntegrationSummary> {
    // What the steps last told the Task about this very attempt.
    let told = task.and_then(|task| match &task.condition {
        db::TaskCondition::Parked {
            primary: db::ParkReason::Integration { reason },
            ..
        } if reason.attempt_id().as_str() == row.attempt_id => Some(reason),
        _ => None,
    });
    let gate = task
        .and_then(task_row_gate)
        .or(project_paused.then_some(GateCause::ProjectPaused));
    let wait = |code: IntegrationWaitCode, message: Option<String>, machine: Option<&str>| {
        let on_machine = on_a_machine(code);
        let machine_id = machine
            .map(str::to_owned)
            .or_else(|| row.machine_id.clone())
            .filter(|_| on_machine);
        IntegrationWait {
            code,
            message,
            machine_name: row
                .machine_name
                .clone()
                .filter(|_| machine_id.is_some() && machine_id == row.machine_id),
            machine_id,
            retry_at: row.available_at.clone(),
        }
    };
    let gate_wait =
        |cause: GateCause| wait(gate_code(cause), Some(cause.message().to_owned()), None);
    let waiting = || {
        let wait = if row.cancel_requested {
            Some(wait(IntegrationWaitCode::CancelRequested, None, None))
        } else if let Some(cause) = gate {
            Some(gate_wait(cause))
        } else {
            match row.queue_state {
                Some(db::IntegrationQueueState::Suspended) => Some(wait(
                    IntegrationWaitCode::QueueSuspended,
                    row.queue_error.clone(),
                    None,
                )),
                Some(db::IntegrationQueueState::Quarantined) => Some(wait(
                    IntegrationWaitCode::QueueQuarantined,
                    row.queue_error.clone(),
                    None,
                )),
                _ if row.ahead.is_some_and(|ahead| ahead > 0) => {
                    Some(wait(IntegrationWaitCode::Queue, None, None))
                }
                _ => None,
            }
        };
        (IntegrationMemberState::Waiting, None, wait)
    };
    let owned = |phase: IntegrationPhase| (IntegrationMemberState::Owned, Some(phase), None);
    let (state, phase, wait) = match row.state {
        S::Queued | S::PathWait => waiting(),
        S::Validating => owned(IntegrationPhase::Validating),
        S::Rebasing => owned(IntegrationPhase::Rebasing),
        S::Checking => owned(IntegrationPhase::Checking),
        // The attempt records that a Task step decides; which decision it
        // waits for (a carried review or the permit) is what the step said.
        S::AwaitingTaskStep => owned(match told {
            Some(IntegrationReason::Owned { phase, .. })
                if matches!(
                    phase,
                    IntegrationPhase::AwaitingCarry | IntegrationPhase::AwaitingAuthorization
                ) =>
            {
                phase.clone()
            }
            _ => IntegrationPhase::AwaitingAuthorization,
        }),
        S::ReadyFf | S::FfInflight => owned(IntegrationPhase::FastForwarding),
        S::Reconciling => owned(IntegrationPhase::Reconciling),
        S::Applied => (IntegrationMemberState::Applied, None, None),
        S::Ejected => match told {
            Some(IntegrationReason::CandidateCheckFailed { message, .. }) => (
                IntegrationMemberState::CandidateCheckFailed,
                None,
                Some(wait(
                    IntegrationWaitCode::OwnerRequired,
                    non_empty(message),
                    None,
                )),
            ),
            _ if row.failure_kind == Some(IntegrationFailureKind::CandidateCheckFailed) => {
                (IntegrationMemberState::CandidateCheckFailed, None, None)
            }
            _ => (IntegrationMemberState::Repair, None, None),
        },
        S::NeedsReview => (IntegrationMemberState::ReviewRequired, None, None),
        S::Parked | S::Quarantined => {
            let message = row.failure_message.as_deref().unwrap_or_default();
            let (reason, detail) = park_reason(message);
            match (row.state, told, reason) {
                // Parked under the Task gate: the Task is in line behind its
                // own stop, which is the reason and carries the action.
                (S::Parked, _, Some(ParkReason::Gate(cause))) => (
                    IntegrationMemberState::Waiting,
                    None,
                    Some(gate_wait(gate.unwrap_or(cause))),
                ),
                (
                    _,
                    Some(IntegrationReason::Deferred {
                        cause,
                        owner_id,
                        message,
                        ..
                    }),
                    _,
                ) => (
                    IntegrationMemberState::Deferred,
                    None,
                    Some(wait(
                        deferral_code(cause),
                        non_empty(message),
                        owner_id.as_deref(),
                    )),
                ),
                (S::Quarantined, _, _) => (
                    IntegrationMemberState::Deferred,
                    None,
                    Some(wait(
                        IntegrationWaitCode::UnresolvedResult,
                        non_empty(message),
                        None,
                    )),
                ),
                (_, _, reason) => {
                    let code = match reason {
                        Some(ParkReason::TargetNotReady) => IntegrationWaitCode::OwnerOffline,
                        Some(ParkReason::TargetDirty) => IntegrationWaitCode::TargetDirty,
                        Some(ParkReason::ExternalMovesExhausted | ParkReason::RoundsExhausted) => {
                            IntegrationWaitCode::BudgetExhausted
                        }
                        // A counted retry that is still scheduled is Forge's
                        // own; one with no time left is the owner's.
                        Some(
                            ParkReason::Infrastructure
                            | ParkReason::Timeout
                            | ParkReason::StepFailed,
                        ) if row.available_at.is_some() => IntegrationWaitCode::Infrastructure,
                        Some(
                            ParkReason::Infrastructure
                            | ParkReason::Timeout
                            | ParkReason::StepFailed,
                        ) => IntegrationWaitCode::BudgetExhausted,
                        Some(_) => IntegrationWaitCode::OwnerRequired,
                        None => match row.failure_kind {
                            Some(
                                IntegrationFailureKind::Infrastructure
                                | IntegrationFailureKind::Timeout,
                            ) => IntegrationWaitCode::Infrastructure,
                            Some(IntegrationFailureKind::TargetUnavailable) => {
                                IntegrationWaitCode::OwnerOffline
                            }
                            Some(IntegrationFailureKind::NeedsFact) => {
                                IntegrationWaitCode::UnresolvedResult
                            }
                            _ => IntegrationWaitCode::OwnerRequired,
                        },
                    };
                    (
                        IntegrationMemberState::Deferred,
                        None,
                        Some(wait(code, non_empty(detail), None)),
                    )
                }
            }
        }
        S::Completed | S::Cancelled | S::Superseded => return None,
    };
    let action_hint = match (&state, wait.as_ref().map(|wait| wait.code)) {
        (IntegrationMemberState::Deferred, _) => Some(IntegrationActionHint::Retry),
        (_, Some(IntegrationWaitCode::TaskHeld)) => Some(IntegrationActionHint::Release),
        (_, Some(IntegrationWaitCode::ProjectPaused)) => Some(IntegrationActionHint::ResumeProject),
        _ => None,
    };
    let timings = row.timings.as_ref();
    Some(TaskIntegrationSummary {
        attempt_id: row.attempt_id.clone(),
        queue_id: row.queue_id.clone(),
        repo_id: row.repo_id.clone(),
        target_branch: row.target_branch.clone(),
        queue_state: row.queue_state.map(queue_state),
        position: match state {
            IntegrationMemberState::Waiting | IntegrationMemberState::Owned => row.ahead,
            _ => None,
        },
        state,
        phase,
        wait,
        action_hint,
        timings: IntegrationTimings {
            enqueued_at: row.enqueued_at.clone(),
            head_since: row.started_at.clone().filter(|_| row.is_head),
            rounds: timings.map_or(0, |timings| timings.rounds),
            queued_ms: timings.and_then(|timings| timings.queued_ms),
            rebase_ms: timings.and_then(|timings| timings.rebase_ms),
            check_ms: timings.and_then(|timings| match &timings.check {
                Some(IntegrationCheckTiming::Ran { run_ms, .. }) => Some(*run_ms),
                _ => None,
            }),
            fast_forward_ms: timings.and_then(|timings| timings.ff_ms),
        },
    })
}

/// The summaries of a page of Tasks from the one batched read of their
/// attempts (`db::integration_members_of_tasks`), keyed by Task id.
pub fn task_integration_summaries<'a>(
    rows: &[IntegrationMemberRead],
    tasks: impl IntoIterator<Item = &'a db::Task>,
    project_paused: bool,
) -> HashMap<String, TaskIntegrationSummary> {
    let rows = rows
        .iter()
        .map(|row| (row.task_ref.as_str(), row))
        .collect::<HashMap<_, _>>();
    tasks
        .into_iter()
        .filter_map(|task| {
            let row = rows.get(task.id.as_str())?;
            Some((
                task.id.clone(),
                task_integration_summary(row, Some(task), project_paused)?,
            ))
        })
        .collect()
}

async fn project_paused(db: &SqliteDb, project_id: &str) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM project WHERE id=? AND paused_at IS NOT NULL)",
    )
    .bind(project_id)
    .fetch_one(db.pool())
    .await
    .map_err(db::DbError::from)?)
}

/// One Task's place in the merge queue; `None` without an attempt in progress.
pub async fn task_integration(
    db: &SqliteDb,
    task: &db::Task,
) -> Result<Option<TaskIntegrationSummary>> {
    let rows = db::integration_members_of_tasks(db.pool(), &[task.id.as_str()]).await?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let paused = project_paused(db, &task.project_id).await?;
    Ok(task_integration_summary(row, Some(task), paused))
}

/// A page of one repository's merge queues with their members. The caller
/// has already decided that the repository's Project is visible; a queue and
/// every member in it belong to that one Project. Four statements whatever
/// the page holds.
pub async fn repo_integration_queues(
    db: &SqliteDb,
    repo: &db::Repo,
    cursor: Option<&str>,
    limit: i64,
) -> Result<IntegrationQueuesResponse> {
    let page = db::integration_queues_of_repo(db.pool(), &repo.id, cursor, limit).await?;
    let queue_ids = page
        .items
        .iter()
        .map(|queue| queue.id.as_str())
        .collect::<Vec<_>>();
    let members = db::integration_members_of_queues(
        db.pool(),
        &queue_ids,
        db::INTEGRATION_QUEUE_MEMBER_LIMIT,
    )
    .await?;
    let task_ids = members
        .iter()
        .map(|member| member.task_ref.as_str())
        .collect::<Vec<_>>();
    let tasks = if task_ids.is_empty() {
        Vec::new()
    } else {
        db.get_tasks_by_ids(&task_ids).await?
    };
    let tasks = tasks
        .iter()
        .map(|task| (task.id.as_str(), task))
        .collect::<HashMap<_, _>>();
    let paused = project_paused(db, &repo.project_id).await?;
    let mut by_queue = HashMap::<&str, Vec<&IntegrationMemberRead>>::new();
    for member in &members {
        if let Some(queue_id) = member.queue_id.as_deref() {
            by_queue.entry(queue_id).or_default().push(member);
        }
    }
    let has_more = page.next_cursor.is_some();
    let items = page
        .items
        .iter()
        .map(|queue| {
            let rows = by_queue.remove(queue.id.as_str()).unwrap_or_default();
            let head = rows.iter().find(|row| row.is_head);
            let member_count = rows.first().map_or(0, |row| row.queue_members);
            let members = rows
                .iter()
                .filter_map(|row| {
                    let task = tasks.get(row.task_ref.as_str()).copied();
                    Some(IntegrationQueueMemberResponse {
                        task_id: row.task_ref.clone(),
                        task_title: task.map(|task| task.title.clone()),
                        task_status: task.map(|task| task.status.clone()),
                        integration: task_integration_summary(row, task, paused)?,
                    })
                })
                .collect::<Vec<_>>();
            let state = queue_state(queue.state);
            IntegrationQueueResponse {
                id: queue.id.clone(),
                repo_id: queue.repo_id.clone(),
                target_branch: queue.target_branch.clone(),
                state_reason: (state != IntegrationQueueState::Open).then(|| {
                    IntegrationQueueStateReason {
                        code: queue
                            .last_error_kind
                            .map_or_else(|| queue.state.to_string(), |kind| kind.to_string()),
                        message: queue.last_error.clone(),
                    }
                }),
                state,
                head_task_id: head.map(|row| row.task_ref.clone()),
                head_attempt_id: queue.head_attempt_id.clone(),
                head_since: head.and_then(|row| row.started_at.clone()),
                members_truncated: member_count as usize > members.len(),
                member_count,
                members,
            }
        })
        .collect();
    Ok(IntegrationQueuesResponse {
        items,
        next_cursor: page.next_cursor,
        has_more,
    })
}
