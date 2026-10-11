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
) -> Option<TaskIntegrationSummary> {
    let project_paused = row.project_paused;
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
                // The cause is the one that holds NOW (the Task's row and its
                // Project), never the one the attempt was parked under: once
                // the stop is lifted the worker puts the attempt back in line
                // by itself, and until it has, nothing is said to stop it.
                (S::Parked, _, Some(ParkReason::Gate(_))) => {
                    (IntegrationMemberState::Waiting, None, gate.map(gate_wait))
                }
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
    // The hint is never a rule of its own. `retry` is the action system's
    // own decision to offer `integration_retry` for this attempt
    // (`task_actions::integration_retry_offered`, the function the offer is
    // built from): an attempt the worker parked a moment ago, whose Task the
    // park step has not told yet, has no hint, because the action would be
    // refused. `release` is the owner's Hold on the Task, which the action
    // system releases (proved against `available_actions` by test).
    // `resume_project` is a Project action and no Task
    // offer: it is said only while the member's own Project is paused.
    let action_hint = match wait.as_ref().map(|wait| wait.code) {
        _ if task.is_some_and(|task| {
            crate::task_actions::integration_retry_offered(task, &row.attempt_id)
        }) =>
        {
            Some(IntegrationActionHint::Retry)
        }
        Some(IntegrationWaitCode::TaskHeld) if matches!(gate, Some(GateCause::Held)) => {
            Some(IntegrationActionHint::Release)
        }
        Some(IntegrationWaitCode::ProjectPaused) if project_paused => {
            Some(IntegrationActionHint::ResumeProject)
        }
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
) -> HashMap<String, TaskIntegrationSummary> {
    let rows = rows
        .iter()
        .map(|row| (row.task_ref.as_str(), row))
        .collect::<HashMap<_, _>>();
    tasks
        .into_iter()
        .filter_map(|task| {
            let row = rows.get(task.id.as_str())?;
            Some((task.id.clone(), task_integration_summary(row, Some(task))?))
        })
        .collect()
}

/// One Task's place in the merge queue; `None` without an attempt in
/// progress. One statement: the member row carries its queue, its machine
/// and whether its Project is paused.
pub async fn task_integration(
    db: &SqliteDb,
    task: &db::Task,
) -> Result<Option<TaskIntegrationSummary>> {
    let rows = db::integration_members_of_tasks(db.pool(), &[task.id.as_str()]).await?;
    Ok(rows
        .first()
        .and_then(|row| task_integration_summary(row, Some(task))))
}

/// A page of one repository's merge queues with their members, as a reader
/// of `project_id` may see them. The caller has already decided that this
/// Project is visible to the reader.
///
/// Admission puts a Task in the queue of a repository of its own Project and
/// refuses any other, so every member of a queue belongs to one Project. This
/// read does not rest on that: a member is listed only if it belongs to
/// `project_id`. A member of any other Project (an agent whose Project names
/// another Project's repository as its checkout reads that repository's
/// queues) is still counted, in `member_count` and in every `position`,
/// because it is really in the line, and it is never listed: no Task id, no
/// title, no status, no attempt id, not even as the head. A Task that was
/// deleted keeps its place and its id (its attempt is still in the line) and
/// loses its title and status.
///
/// Three statements whatever the page holds.
pub async fn repo_integration_queues(
    db: &SqliteDb,
    repo_id: &str,
    project_id: &str,
    target_branch: Option<&str>,
    cursor: Option<&str>,
    limit: i64,
) -> Result<IntegrationQueuesResponse> {
    let page =
        db::integration_queues_of_repo(db.pool(), repo_id, target_branch, cursor, limit).await?;
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
        .filter(|member| member.project_ref == project_id)
        .map(|member| member.task_ref.as_str())
        .collect::<Vec<_>>();
    let tasks = if task_ids.is_empty() {
        Vec::new()
    } else {
        db.get_tasks_by_ids(&task_ids).await?
    };
    let tasks = tasks
        .iter()
        .filter(|task| task.deleted_at.is_none() && task.project_id == project_id)
        .map(|task| (task.id.as_str(), task))
        .collect::<HashMap<_, _>>();
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
            let visible = |row: &&&IntegrationMemberRead| row.project_ref == project_id;
            let head = rows.iter().find(|row| row.is_head);
            let shown_head = head.filter(visible);
            let member_count = rows.first().map_or(0, |row| row.queue_members);
            let read = rows.len();
            let members = rows
                .iter()
                .filter(visible)
                .filter_map(|row| {
                    let task = tasks.get(row.task_ref.as_str()).copied();
                    Some(IntegrationQueueMemberResponse {
                        task_id: row.task_ref.clone(),
                        task_title: task.map(|task| task.title.clone()),
                        task_status: task.map(|task| task.status.clone()),
                        integration: task_integration_summary(row, task)?,
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
                head_task_id: shown_head.map(|row| row.task_ref.clone()),
                head_attempt_id: shown_head.map(|row| row.attempt_id.clone()),
                head_since: head.and_then(|row| row.started_at.clone()),
                members_truncated: member_count as usize > read,
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

/// The merge queues an agent bound to `project_id` reads (`repo.integration_queue`
/// natively, `forge_get_integration_queue` on `POST /mcp`): the queues of the
/// Project's repository as that Project sees them, or the one queue of
/// `target_branch`. One function, so both tools answer alike.
pub async fn project_integration_queues(
    db: &SqliteDb,
    project_id: &str,
    target_branch: Option<&str>,
    limit: Option<u64>,
) -> Result<serde_json::Value> {
    use db::{ProjectRepo, RepoRepo};
    let limit = limit.unwrap_or(20).clamp(1, 100) as i64;
    let repo_id = ProjectRepo::get_by_id(db, project_id)
        .await?
        .and_then(|project| project.primary_repo_id);
    let repo = match repo_id.as_deref() {
        Some(repo_id) => RepoRepo::get_by_id(db, repo_id).await?,
        None => None,
    };
    let Some(repo) = repo else {
        return Ok(serde_json::json!({
            "project_id": project_id,
            "repo_id": serde_json::Value::Null,
            "items": [],
            "has_more": false,
        }));
    };
    let page =
        repo_integration_queues(db, &repo.id, project_id, target_branch, None, limit).await?;
    Ok(serde_json::json!({
        "project_id": project_id,
        "repo_id": repo.id,
        "items": page.items,
        "has_more": page.has_more,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use api_types::IntegrationAttemptId;
    use db::{IntegrationQueueRepo, TaskRepo};

    struct Fixture {
        db: SqliteDb,
        task: db::Task,
        attempt: String,
    }
    async fn fixture() -> Fixture {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = SqliteDb::new(pool);
        let now = db::now_rfc3339();
        sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES('p','p',?,?)")
            .bind(&now)
            .bind(&now)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO repo(id,project_id,name,default_branch,created_at,updated_at) VALUES('r','p','r','main',?,?)")
            .bind(&now)
            .bind(&now)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t','p','t','merging',?,?)")
            .bind(&now)
            .bind(&now)
            .execute(db.pool())
            .await
            .unwrap();
        let queue = db
            .create_or_get_integration_queue("r", "main")
            .await
            .unwrap();
        sqlx::query(
            "UPDATE integration_queue SET state='open',last_error_kind=NULL,last_error=NULL",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let attempt = db
            .admit_integration_attempt(db::IntegrationAttempt::new(
                Some(queue.id),
                "t".into(),
                "p".into(),
                "admission".into(),
                "merging".into(),
                0,
                1,
            ))
            .await
            .unwrap()
            .id;
        let task = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
        Fixture { db, task, attempt }
    }
    impl Fixture {
        async fn attempt(&self, assignments: &str) {
            sqlx::query(&format!(
                "UPDATE integration_attempt SET {assignments} WHERE id=?"
            ))
            .bind(&self.attempt)
            .execute(self.db.pool())
            .await
            .unwrap();
        }
        /// The summary of the Task as if a Task step had told it `reason`.
        async fn told(&self, reason: Option<IntegrationReason>) -> TaskIntegrationSummary {
            let mut task = self.task.clone();
            if let Some(reason) = reason {
                task.condition = db::TaskCondition::Parked {
                    resume: db::task_condition::ConditionContinuation::Integration {
                        attempt_id: reason.attempt_id().clone(),
                    },
                    primary: db::ParkReason::Integration { reason },
                    additional: Vec::new(),
                    since: None,
                    evidence: Default::default(),
                };
            }
            task_integration(&self.db, &task).await.unwrap().unwrap()
        }
        fn id(&self) -> IntegrationAttemptId {
            IntegrationAttemptId::new(self.attempt.clone())
        }
    }

    #[tokio::test]
    async fn the_step_statement_refines_what_the_attempt_alone_cannot_say() {
        let fixture = fixture().await;

        // Which decision the head waits for is the step's knowledge.
        fixture.attempt("state='awaiting_task_step'").await;
        assert_eq!(
            fixture.told(None).await.phase,
            Some(IntegrationPhase::AwaitingAuthorization)
        );
        let carried = fixture
            .told(Some(IntegrationReason::Owned {
                attempt_id: fixture.id(),
                phase: IntegrationPhase::AwaitingCarry,
            }))
            .await;
        assert_eq!(carried.state, IntegrationMemberState::Owned);
        assert_eq!(carried.phase, Some(IntegrationPhase::AwaitingCarry));
        // A stale statement (the head has moved on) never overrides the attempt.
        fixture.attempt("state='ff_inflight'").await;
        assert_eq!(
            fixture
                .told(Some(IntegrationReason::Owned {
                    attempt_id: fixture.id(),
                    phase: IntegrationPhase::AwaitingCarry,
                }))
                .await
                .phase,
            Some(IntegrationPhase::FastForwarding)
        );

        // Sent back: a repair, unless the candidate's check failed.
        fixture.attempt("state='ejected'").await;
        let repair = fixture.told(None).await;
        assert_eq!(repair.state, IntegrationMemberState::Repair);
        assert_eq!((repair.position, repair.action_hint), (None, None));
        let failed = fixture
            .told(Some(IntegrationReason::CandidateCheckFailed {
                attempt_id: fixture.id(),
                check: "unit".into(),
                message: "candidate exit 1".into(),
            }))
            .await;
        assert_eq!(failed.state, IntegrationMemberState::CandidateCheckFailed);
        assert_eq!(
            failed.wait.and_then(|wait| wait.message).as_deref(),
            Some("candidate exit 1")
        );
        fixture
            .attempt("state='ejected',failure_kind='candidate_check_failed'")
            .await;
        assert_eq!(
            fixture.told(None).await.state,
            IntegrationMemberState::CandidateCheckFailed
        );
        fixture
            .attempt("state='needs_review',failure_kind=NULL")
            .await;
        assert_eq!(
            fixture.told(None).await.state,
            IntegrationMemberState::ReviewRequired
        );
    }

    #[tokio::test]
    async fn a_deferral_names_its_cause_its_machine_and_the_retry() {
        let fixture = fixture().await;
        fixture
            .attempt("state='parked',failure_kind='target_unavailable',failure_message='target_not_ready: the default checkout is not connected'")
            .await;
        // From the attempt alone: the worker's own park reason. The park
        // step has not told the Task yet, so the action system does not
        // offer the retry and the summary does not hint at it.
        let parked = fixture.told(None).await;
        assert_eq!(parked.state, IntegrationMemberState::Deferred);
        let wait = parked.wait.unwrap();
        assert_eq!(wait.code, IntegrationWaitCode::OwnerOffline);
        assert_eq!(
            wait.message.as_deref(),
            Some("the default checkout is not connected")
        );
        assert_eq!(parked.action_hint, None);

        // What the park step told the Task wins: cause, machine, message.
        let told = fixture
            .told(Some(IntegrationReason::Deferred {
                attempt_id: fixture.id(),
                cause: IntegrationDeferralCause::OwnerOffline,
                owner_id: Some("daemon-7".into()),
                message: "build-box is not connected".into(),
            }))
            .await;
        assert_eq!(told.action_hint, Some(IntegrationActionHint::Retry));
        let wait = told.wait.unwrap();
        assert_eq!(wait.code, IntegrationWaitCode::OwnerOffline);
        assert_eq!(wait.machine_id.as_deref(), Some("daemon-7"));
        assert_eq!(wait.message.as_deref(), Some("build-box is not connected"));
        // A cause with no machine carries none.
        let dirty = fixture
            .told(Some(IntegrationReason::Deferred {
                attempt_id: fixture.id(),
                cause: IntegrationDeferralCause::TargetDirty,
                owner_id: Some("daemon-7".into()),
                message: "uncommitted changes".into(),
            }))
            .await;
        let wait = dirty.wait.unwrap();
        assert_eq!(wait.code, IntegrationWaitCode::TargetDirty);
        assert_eq!(wait.machine_id, None);

        // A statement about an earlier attempt of the Task says nothing
        // about this one.
        let other = fixture
            .told(Some(IntegrationReason::Deferred {
                attempt_id: IntegrationAttemptId::new("an-earlier-attempt"),
                cause: IntegrationDeferralCause::TargetDirty,
                owner_id: None,
                message: "old".into(),
            }))
            .await;
        assert_eq!(
            other.wait.map(|wait| wait.code),
            Some(IntegrationWaitCode::OwnerOffline)
        );

        // Counted retries: Forge's own while one is scheduled, the owner's after.
        fixture
            .attempt("failure_kind='infrastructure',failure_message='infrastructure: owner did not answer',available_at='2026-10-01T00:05:00+00:00'")
            .await;
        let retrying = fixture.told(None).await.wait.unwrap();
        assert_eq!(retrying.code, IntegrationWaitCode::Infrastructure);
        assert_eq!(
            retrying.retry_at.as_deref(),
            Some("2026-10-01T00:05:00+00:00")
        );
        fixture.attempt("available_at=NULL").await;
        assert_eq!(
            fixture.told(None).await.wait.map(|wait| wait.code),
            Some(IntegrationWaitCode::BudgetExhausted)
        );
        fixture.attempt("state='quarantined'").await;
        assert_eq!(
            fixture.told(None).await.wait.map(|wait| wait.code),
            Some(IntegrationWaitCode::UnresolvedResult)
        );
    }

    #[tokio::test]
    async fn a_task_its_owner_stopped_waits_in_line_with_that_cause() {
        let fixture = fixture().await;
        // In line, nothing ahead, nothing stops it.
        let free = fixture.told(None).await;
        assert_eq!(
            (free.state, free.position, free.wait),
            (IntegrationMemberState::Waiting, Some(0), None)
        );
        // The owner's Hold on the Task row is the same gate the worker reads.
        let mut held = fixture.task.clone();
        held.condition = db::TaskCondition::Parked {
            primary: db::ParkReason::Held {
                actor: "user".into(),
            },
            additional: Vec::new(),
            resume: db::task_condition::ConditionContinuation::Reconcile,
            since: None,
            evidence: Default::default(),
        };
        let summary = task_integration(&fixture.db, &held).await.unwrap().unwrap();
        assert_eq!(summary.state, IntegrationMemberState::Waiting);
        assert_eq!(
            summary.wait.map(|wait| wait.code),
            Some(IntegrationWaitCode::TaskHeld)
        );
        assert_eq!(summary.action_hint, Some(IntegrationActionHint::Release));
        // A cancel request is being applied: nothing else is said.
        fixture
            .attempt("cancel_requested_at='2026-10-01T00:00:00+00:00'")
            .await;
        assert_eq!(
            fixture.told(None).await.wait.map(|wait| wait.code),
            Some(IntegrationWaitCode::CancelRequested)
        );
        // A finished attempt is no summary.
        fixture
            .attempt("state='completed',current=0,completed_at=updated_at")
            .await;
        assert_eq!(
            task_integration(&fixture.db, &fixture.task).await.unwrap(),
            None
        );
    }

    /// Every state an attempt in progress can be in, under every queue state
    /// and place in line, against the one summary. `None` is "no summary".
    #[tokio::test]
    async fn the_summary_says_what_the_attempt_and_its_queue_say_in_every_state() {
        use IntegrationMemberState as M;
        use IntegrationPhase as P;
        use IntegrationWaitCode as W;
        let fixture = fixture().await;
        // Two more members behind the fixture's: `t` is first in line.
        let mut behind = Vec::new();
        for task in ["t2", "t3"] {
            sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES(?,'p',?,'merging','2026-10-01T00:00:00+00:00','2026-10-01T00:00:00+00:00')")
                .bind(task)
                .bind(task)
                .execute(fixture.db.pool())
                .await
                .unwrap();
            let queue: String = sqlx::query_scalar("SELECT id FROM integration_queue")
                .fetch_one(fixture.db.pool())
                .await
                .unwrap();
            fixture
                .db
                .admit_integration_attempt(db::IntegrationAttempt::new(
                    Some(queue),
                    task.into(),
                    "p".into(),
                    format!("admission-{task}"),
                    "merging".into(),
                    0,
                    1,
                ))
                .await
                .unwrap();
            behind.push(
                TaskRepo::get_by_id(&fixture.db, task, false)
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        let queue = |assignments: &'static str| {
            let db = fixture.db.clone();
            let head = fixture.attempt.clone();
            async move {
                sqlx::query(&format!("UPDATE integration_queue SET {assignments}"))
                    .bind(head)
                    .execute(db.pool())
                    .await
                    .unwrap();
            }
        };
        type Expected = Option<(M, Option<P>, Option<u32>, Option<W>)>;
        // (attempt columns, at the head, expected of the member itself)
        let table: Vec<(&str, bool, Expected)> = vec![
            ("state='queued'", false, Some((M::Waiting, None, Some(0), None))),
            ("state='path_wait'", false, Some((M::Waiting, None, Some(0), None))),
            ("state='validating'", true, Some((M::Owned, Some(P::Validating), Some(0), None))),
            ("state='rebasing'", true, Some((M::Owned, Some(P::Rebasing), Some(0), None))),
            ("state='checking'", true, Some((M::Owned, Some(P::Checking), Some(0), None))),
            ("state='awaiting_task_step'", true, Some((M::Owned, Some(P::AwaitingAuthorization), Some(0), None))),
            ("state='ready_ff'", true, Some((M::Owned, Some(P::FastForwarding), Some(0), None))),
            ("state='ff_inflight'", true, Some((M::Owned, Some(P::FastForwarding), Some(0), None))),
            ("state='reconciling'", true, Some((M::Owned, Some(P::Reconciling), Some(0), None))),
            ("state='applied'", true, Some((M::Applied, None, None, None))),
            ("state='needs_review'", false, Some((M::ReviewRequired, None, None, None))),
            ("state='ejected'", false, Some((M::Repair, None, None, None))),
            ("state='ejected',failure_kind='candidate_check_failed'", false, Some((M::CandidateCheckFailed, None, None, None))),
            ("state='quarantined',failure_message='the fast-forward has no answer'", true, Some((M::Deferred, None, None, Some(W::UnresolvedResult)))),
            ("state='parked',failure_message='target_not_ready: gone'", false, Some((M::Deferred, None, None, Some(W::OwnerOffline)))),
            ("state='parked',failure_message='target_dirty: local changes'", false, Some((M::Deferred, None, None, Some(W::TargetDirty)))),
            ("state='parked',failure_message='worktree_dirty: local changes'", false, Some((M::Deferred, None, None, Some(W::OwnerRequired)))),
            ("state='parked',failure_message='unsupported_conflict: binary'", false, Some((M::Deferred, None, None, Some(W::OwnerRequired)))),
            ("state='parked',failure_message='transfer_too_large: 3 GiB'", false, Some((M::Deferred, None, None, Some(W::OwnerRequired)))),
            ("state='parked',failure_message='external_target_moves_exhausted: 5'", false, Some((M::Deferred, None, None, Some(W::BudgetExhausted)))),
            ("state='parked',failure_message='rounds_exhausted: 5'", false, Some((M::Deferred, None, None, Some(W::BudgetExhausted)))),
            ("state='parked',failure_message='infrastructure: no answer',available_at='2026-10-01T00:05:00+00:00'", false, Some((M::Deferred, None, None, Some(W::Infrastructure)))),
            ("state='parked',failure_message='timeout: 600 s',available_at='2026-10-01T00:05:00+00:00'", false, Some((M::Deferred, None, None, Some(W::Infrastructure)))),
            ("state='parked',failure_message='task_step_failed: no',available_at=NULL", false, Some((M::Deferred, None, None, Some(W::BudgetExhausted)))),
            ("state='parked',failure_kind='needs_fact',failure_message='not a known reason'", false, Some((M::Deferred, None, None, Some(W::UnresolvedResult)))),
            // Parked under a stop of the Task that is no longer there: the
            // worker is about to put it back in line; nothing stops it.
            ("state='parked',failure_message='task_held: on hold'", false, Some((M::Waiting, None, None, None))),
            ("state='parked',failure_message='project_paused: paused'", false, Some((M::Waiting, None, None, None))),
            ("state='completed',current=0,completed_at=updated_at", false, None),
            ("state='cancelled',current=0,completed_at=updated_at", false, None),
            ("state='superseded',current=0,completed_at=updated_at", false, None),
        ];
        // Every state the attempt table admits is in the table above.
        for state in [
            S::Queued,
            S::PathWait,
            S::Validating,
            S::Rebasing,
            S::Checking,
            S::AwaitingTaskStep,
            S::ReadyFf,
            S::FfInflight,
            S::Reconciling,
            S::Applied,
            S::Ejected,
            S::NeedsReview,
            S::Parked,
            S::Quarantined,
            S::Completed,
            S::Cancelled,
            S::Superseded,
        ] {
            assert!(
                table
                    .iter()
                    .any(|(columns, ..)| columns.starts_with(&format!("state='{state}'"))),
                "{state} has no row"
            );
        }
        for (columns, at_head, expected) in table {
            fixture
                .attempt(&format!(
                    "current=1,completed_at=NULL,failure_kind=NULL,failure_message=NULL,available_at=NULL,{columns}"
                ))
                .await;
            queue(if at_head {
                "head_attempt_id=?"
            } else {
                "head_attempt_id=NULL AND ? IS NOT NULL"
            })
            .await;
            let summary = task_integration(&fixture.db, &fixture.task).await.unwrap();
            assert_eq!(
                summary.as_ref().map(|summary| (
                    summary.state,
                    summary.phase.clone(),
                    summary.position,
                    summary.wait.as_ref().map(|wait| wait.code)
                )),
                expected,
                "{columns}"
            );
            // No step has told the Task anything: no action is hinted.
            assert_eq!(
                summary.and_then(|summary| summary.action_hint),
                None,
                "{columns}"
            );
            // The members behind: one ahead for each waiter in front, plus
            // the head. A parked or sent-back member is ahead of nobody.
            let in_line =
                columns.starts_with("state='queued'") || columns.starts_with("state='path_wait'");
            let ahead = u32::from(at_head) + u32::from(in_line);
            for (n, task) in behind.iter().enumerate() {
                let summary = task_integration(&fixture.db, task).await.unwrap().unwrap();
                assert_eq!(
                    (summary.state, summary.position),
                    (M::Waiting, Some(ahead + n as u32)),
                    "{columns}: member {n} behind"
                );
                assert_eq!(
                    summary.wait.map(|wait| wait.code),
                    (ahead + n as u32 > 0).then_some(W::Queue),
                    "{columns}: member {n} behind"
                );
            }
        }

        // The queue's own state is the wait of every member in line, and of
        // none that is not in line.
        fixture
            .attempt("current=1,completed_at=NULL,state='queued',failure_message=NULL")
            .await;
        for (state, code, queue_state) in [
            (
                "suspended",
                W::QueueSuspended,
                IntegrationQueueState::Suspended,
            ),
            (
                "quarantined",
                W::QueueQuarantined,
                IntegrationQueueState::Quarantined,
            ),
        ] {
            sqlx::query("UPDATE integration_queue SET head_attempt_id=NULL,state=?,last_error_kind='target_unavailable',last_error='not ready'")
                .bind(state)
                .execute(fixture.db.pool())
                .await
                .unwrap();
            for task in std::iter::once(&fixture.task).chain(&behind) {
                let summary = task_integration(&fixture.db, task).await.unwrap().unwrap();
                assert_eq!(summary.queue_state, Some(queue_state));
                let wait = summary.wait.unwrap();
                assert_eq!(
                    (wait.code, wait.message.as_deref()),
                    (code, Some("not ready"))
                );
                assert_eq!(summary.action_hint, None);
            }
        }
        sqlx::query(
            "UPDATE integration_queue SET state='open',last_error_kind=NULL,last_error=NULL",
        )
        .execute(fixture.db.pool())
        .await
        .unwrap();
        // A paused Project is the wait of its members, read from the member
        // row itself, and the one hint that is a Project action.
        sqlx::query("UPDATE project SET paused_at='2026-10-01T00:00:00+00:00'")
            .execute(fixture.db.pool())
            .await
            .unwrap();
        let paused = task_integration(&fixture.db, &fixture.task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(paused.wait.map(|wait| wait.code), Some(W::ProjectPaused));
        assert_eq!(
            paused.action_hint,
            Some(IntegrationActionHint::ResumeProject)
        );
        fixture
            .attempt("state='parked',failure_message='project_paused: paused'")
            .await;
        let parked = task_integration(&fixture.db, &fixture.task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                parked.state,
                parked.position,
                parked.wait.map(|wait| wait.code)
            ),
            (M::Waiting, None, Some(W::ProjectPaused))
        );
        // A Task in its merge state that has no attempt yet (the reconciler
        // will admit it) has no summary: `null`, never an error.
        sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('new','p','new','merging','2026-10-01T00:00:00+00:00','2026-10-01T00:00:00+00:00')")
            .execute(fixture.db.pool())
            .await
            .unwrap();
        let unadmitted = TaskRepo::get_by_id(&fixture.db, "new", false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            task_integration(&fixture.db, &unadmitted).await.unwrap(),
            None
        );
    }

    /// The hint is the action system's own answer: for every statement a
    /// step can have made to the Task and every state its attempt can be in,
    /// `retry` is hinted exactly when `available_actions` offers
    /// `integration_retry`, and `release` only when it offers `release`.
    #[tokio::test]
    async fn the_action_hint_is_what_the_action_system_offers() {
        let fixture = fixture().await;
        let mut hinted_retry = 0;
        for condition in crate::task_actions::tests::integration_conditions() {
            for state in [
                "queued",
                "validating",
                "awaiting_task_step",
                "parked",
                "quarantined",
                "ejected",
                "needs_review",
                "applied",
            ] {
                fixture
                    .attempt(&format!(
                        "state='{state}',failure_message='infrastructure: no answer'"
                    ))
                    .await;
                let mut snapshot = crate::task_actions::tests::snapshot("merging", None);
                snapshot.task.condition = condition.clone();
                for (attempt, same) in [("attempt", true), ("an-earlier-attempt", false)] {
                    let mut rows = db::integration_members_of_tasks(fixture.db.pool(), &["t"])
                        .await
                        .unwrap();
                    rows[0].attempt_id = attempt.to_owned();
                    let hint = task_integration_summary(&rows[0], Some(&snapshot.task))
                        .unwrap()
                        .action_hint;
                    let offered = crate::available_actions(&snapshot).iter().any(|offer| {
                        offer.action.verb() == "retry" && offer.reason == "integration_retry"
                    });
                    assert_eq!(
                        hint == Some(IntegrationActionHint::Retry),
                        offered && same,
                        "{state} {condition:?}"
                    );
                    hinted_retry += usize::from(hint == Some(IntegrationActionHint::Retry));
                }
                // The owner's Hold: the action system releases it.
                let mut held = snapshot.clone();
                held.task.condition = db::TaskCondition::Parked {
                    primary: db::ParkReason::Held {
                        actor: "user".into(),
                    },
                    additional: Vec::new(),
                    resume: db::task_condition::ConditionContinuation::Reconcile,
                    since: None,
                    evidence: Default::default(),
                };
                let rows = db::integration_members_of_tasks(fixture.db.pool(), &["t"])
                    .await
                    .unwrap();
                let hint = task_integration_summary(&rows[0], Some(&held.task))
                    .unwrap()
                    .action_hint;
                let offers = crate::available_actions(&held);
                if hint == Some(IntegrationActionHint::Release) {
                    assert!(
                        offers.iter().any(|offer| offer.action.verb() == "release"),
                        "{state}"
                    );
                }
                assert_ne!(hint, Some(IntegrationActionHint::Retry), "{state}");
            }
        }
        assert!(hinted_retry > 0, "no deferred statement was tried");
    }
}
