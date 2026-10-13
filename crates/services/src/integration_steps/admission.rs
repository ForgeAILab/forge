//! Admission: a Task entering `merging` joins the queue of its repository's
//! target branch (plan 3.2 stage D2b).
//!
//! This is the `run_merge` entry hook's whole effect. It runs inside the
//! hooks step of the `merging` entry, so it is the Task-step writer: it
//! creates (or supersedes) the Task's integration attempt, states `waiting`,
//! and wakes the queue worker after its commit. It performs no Git effect.
//!
//! The attempt records where the candidate lives. Those fields are the
//! authority for the owner that may rebase it (`begin_integration_effect`):
//! `workspace_id`, `placement_id`, `repo_location_id`, `owner_kind`,
//! `daemon_id`, `runtime_id`, `placement_generation` and
//! `original_candidate_sha`.
use crate::{merge_service::MergeService, Result, ServiceError};
use api_types::IntegrationReason;
use db::{
    ConditionStatement, DbError, IntegrationAttempt, IntegrationAttemptState as S,
    IntegrationOwnerKind, IntegrationQueueRepo, SqliteDb,
};
use std::sync::Arc;

/// An attempt carries everything the worker's owner checks read.
fn bound(attempt: &IntegrationAttempt) -> bool {
    attempt.workspace_id.is_some()
        && attempt.placement_id.is_some()
        && attempt.repo_location_id.is_some()
        && attempt.owner_kind.is_some()
        && attempt.placement_generation.is_some()
        && attempt.original_candidate_sha.is_some()
}

pub(crate) fn unbound(attempt: &IntegrationAttempt) -> bool {
    !bound(attempt)
}

/// The attempt's binding is provably gone: the Task's placement is `ready`
/// (recovery verified it, `recovery::reconcile_workspace_placement`) and
/// names another workspace, location, owner, runtime or generation than the
/// attempt was admitted with. A placement that is not ready proves nothing
/// (its machine may only be away): the attempt keeps its binding and its
/// park. This is the predicate of `INTEGRATION_OWNED` in `db::sqlite::schedule`.
///
/// A stale attempt is replaced through admission only; whether it is free
/// to be replaced (not a head, no unsettled effect) is the supersede
/// transaction's decision.
pub(crate) async fn binding_stale(db: &SqliteDb, attempt: &IntegrationAttempt) -> Result<bool> {
    let Some(id) = attempt.placement_id.as_deref() else {
        return Ok(false);
    };
    let Some(placement) = db::WorkspacePlacementRepo::get_by_id(db, id).await? else {
        return Ok(false);
    };
    Ok(placement.state == db::PlacementState::Ready
        && !(attempt.workspace_id.as_deref() == Some(placement.workspace_id.as_str())
            && attempt.repo_location_id.as_deref() == Some(placement.repo_location_id.as_str())
            && attempt.owner_kind
                == Some(match placement.owner_kind {
                    db::PlacementOwnerKind::Server => IntegrationOwnerKind::Server,
                    db::PlacementOwnerKind::Daemon => IntegrationOwnerKind::Daemon,
                })
            && attempt.daemon_id == placement.daemon_id
            && attempt.runtime_id == placement.runtime_id
            && attempt.placement_generation == Some(placement.generation)))
}

/// Admit `task` (in `merging`) to its target's queue and return its current
/// attempt. Idempotent: a hook that runs again for the same status entry,
/// placement and candidate gets the same row.
///
/// An attempt the Task still has from an earlier entry (ejected for repair,
/// sent to review, parked), or one admitted without the owner fields (a
/// shadow row written before the queue was active), is superseded: the
/// successor keeps the Task's place in line (`queue_seq`).
///
/// `VersionConflict`: the earlier attempt still holds its queue slot (it has
/// been told to release; see [`admit_released`]), or another writer got in
/// between.
///
/// This is the one admission function: the `run_merge` entry hook, the
/// import continuation, the reconciler (`reconcile`) and the owner's Retry
/// all call it, so each of them is idempotent and identity-fenced the same
/// way (`admit:<task>:<epoch>[:<n>]`).
pub(crate) async fn admit(
    db: &Arc<SqliteDb>,
    merge_service: &MergeService,
    task: &db::Task,
    replace_released: bool,
) -> Result<IntegrationAttempt> {
    let facts = merge_service.integration_admission_facts(task).await?;
    let epoch: i64 = db.integration_admission_task_status_epoch(&task.id).await?;
    let queue = db
        .create_or_get_integration_queue(&facts.workspace.repo_id, &facts.target_branch)
        .await?;
    let placement = &facts.placement;
    let build = |key: String, reviewed: Option<&str>| {
        let mut attempt = IntegrationAttempt::new(
            Some(queue.id.clone()),
            task.id.clone(),
            task.project_id.clone(),
            key,
            task.status.clone(),
            epoch,
            task.version,
        );
        attempt.execution_id = facts.execution_id.clone();
        attempt.execution_ref = facts.execution_id.clone();
        attempt.workspace_id = Some(facts.workspace.id.clone());
        attempt.workspace_ref = Some(facts.workspace.id.clone());
        attempt.placement_id = Some(placement.id.clone());
        attempt.placement_ref = Some(placement.id.clone());
        attempt.repo_location_id = Some(placement.repo_location_id.clone());
        attempt.repo_location_ref = Some(placement.repo_location_id.clone());
        attempt.owner_kind = Some(match placement.owner_kind {
            db::PlacementOwnerKind::Daemon => IntegrationOwnerKind::Daemon,
            db::PlacementOwnerKind::Server => IntegrationOwnerKind::Server,
        });
        attempt.daemon_id = placement.daemon_id.clone();
        attempt.runtime_id = placement.runtime_id.clone();
        attempt.placement_generation = Some(placement.generation);
        // `original_candidate_sha` is the commit the review passed; the
        // worker checks any other commit before it may merge. It is the
        // admitted HEAD, except for a candidate the old merge path had
        // already rebased or an earlier attempt rebased (see
        // `reviewed_before_rebase`).
        attempt.original_candidate_sha = Some(reviewed.unwrap_or(&facts.candidate_sha).to_owned());
        if reviewed.is_some() {
            attempt.candidate_sha = Some(facts.candidate_sha.clone());
        }
        attempt
    };
    for _ in 0..3 {
        let Some(current) = db.current_integration_attempt(&task.id).await? else {
            let reviewed = reviewed_before_rebase(db, None, task, &facts.candidate_sha).await?;
            let mut successor = build(format!("admit:{}:{epoch}", task.id), reviewed.as_deref());
            successor.predecessor_attempt_id = db
                .unowned_quarantined_import(&task.id)
                .await?
                .map(|import| import.id);
            match db.admit_integration_attempt(successor).await {
                Ok(attempt) => return Ok(attempt),
                // Another admission of this Task got in; read it.
                Err(DbError::VersionConflict) => continue,
                Err(error) => return Err(error.into()),
            }
        };
        let same_entry = current.expected_epoch == epoch
            && current.expected_status == task.status
            && current.queue_id.as_deref() == Some(queue.id.as_str())
            && current.placement_id.as_deref() == Some(placement.id.as_str())
            && current.placement_generation == Some(placement.generation)
            && current.daemon_id == placement.daemon_id
            && current.runtime_id == placement.runtime_id
            && current.repo_location_id.as_deref() == Some(placement.repo_location_id.as_str())
            && current.workspace_id.as_deref() == Some(facts.workspace.id.as_str())
            && current.owner_kind
                == Some(match placement.owner_kind {
                    db::PlacementOwnerKind::Server => IntegrationOwnerKind::Server,
                    db::PlacementOwnerKind::Daemon => IntegrationOwnerKind::Daemon,
                })
            && (current.original_candidate_sha.as_deref() == Some(facts.candidate_sha.as_str())
                || (current.import_source_json.is_none()
                    && current.candidate_sha.as_deref() == Some(facts.candidate_sha.as_str())
                    && current.state == S::Queued));
        // `replace_released` (the reconciler, the owner's Retry): an attempt
        // that left the queue for good while the Task stayed in this entry
        // (quarantined with nothing unsettled; sent back, with the Task then
        // blocked for its owner instead of moved) gets a successor, which
        // keeps the Task's place in line.
        let replaced = replace_released
            && matches!(current.state, S::Quarantined | S::Ejected | S::NeedsReview)
            && current.effect_intent_json.is_none();
        if same_entry && bound(&current) && !replaced {
            return Ok(current);
        }
        // The same status entry, but the row does not carry what this
        // admission would write (an imported or shadow row, or the candidate
        // moved under a retried hook). It is replaced while nothing holds
        // it; a row the worker already drives stays the Task's attempt.
        let same_status_entry =
            current.expected_epoch == epoch && current.expected_status == task.status;
        if current.queue_id.as_deref() != Some(queue.id.as_str()) {
            // The Task's target changed between entries: the old attempt
            // leaves its own queue first (the worker's sweep cancels it).
            let mut tx = db::begin_immediate(db.pool()).await?;
            db::request_task_integration_cancel_in_tx(&mut tx, &task.id, &db::now_rfc3339())
                .await?;
            tx.commit().await?;
            if matches!(
                current.state,
                S::Queued | S::Parked | S::Ejected | S::NeedsReview
            ) && current.effect_intent_json.is_none()
            {
                let mut ended = current.clone();
                ended.state = S::Cancelled;
                match db.transition_integration_attempt(ended).await {
                    Ok(_) | Err(DbError::VersionConflict) => continue,
                    Err(DbError::InvalidTransition) => {}
                    Err(error) => return Err(error.into()),
                }
            }
            return Err(DbError::VersionConflict.into());
        }
        let reviewed =
            reviewed_before_rebase(db, Some(&current), task, &facts.candidate_sha).await?;
        let successor = build(
            format!("admit:{}:{epoch}:{}", task.id, current.attempt_number + 1),
            reviewed.as_deref(),
        );
        match db
            .supersede_integration_attempt(&current.id, current.revision, successor)
            .await
        {
            Ok(attempt) => return Ok(attempt),
            Err(DbError::VersionConflict) => continue,
            Err(DbError::InvalidTransition) if same_status_entry => return Ok(current),
            // The attempt of an entry the Task has left is still the head of
            // its queue (the head notices a Task that moved only at its next
            // step, which can be a whole check away), or an effect of it is
            // unsettled. It is told to release now, in the Cancel's own
            // write; `admit_released` waits for the worker to do it.
            Err(DbError::InvalidTransition) => {
                let mut tx = db::begin_immediate(db.pool()).await?;
                db::request_task_integration_cancel_in_tx(&mut tx, &task.id, &db::now_rfc3339())
                    .await?;
                tx.commit().await?;
                return Err(DbError::VersionConflict.into());
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(DbError::VersionConflict.into())
}

/// How long an admission waits for the worker to release the attempt of an
/// entry the Task has left. The release is one driver pass after the cancel
/// flag, and the driver signals it (`IntegrationQueueWorker::notify_released`),
/// so the admission looks again at the release, not at its poll; the bound
/// only covers a worker that is not running. Shorter than a Task step's
/// lease.
const RELEASE_WAIT: std::time::Duration = std::time::Duration::from_secs(20);
const RELEASE_POLL: std::time::Duration = std::time::Duration::from_millis(100);

/// [`admit`], waiting for an older attempt of the Task to leave its queue
/// slot instead of failing the caller's step.
///
/// A Task that re-enters `merging` while the attempt of its previous entry is
/// still the head gets `VersionConflict` from [`admit`] until the worker has
/// released that attempt. Returning that to the hook step would spend one of
/// its attempts per look and, once they are gone, settle the entry as a
/// workspace error although nothing is wrong. The wait here is deterministic:
/// the old attempt is flagged, the worker is woken, and the admission is
/// made as soon as the slot is free. After [`RELEASE_WAIT`] (no worker runs,
/// or the old attempt's fast-forward has an unknown result and may not be
/// released) the conflict is returned and the step retries with back-off.
pub(crate) async fn admit_released(
    db: &Arc<SqliteDb>,
    merge_service: &MergeService,
    task: &db::Task,
    worker: Option<&crate::integration_worker::IntegrationQueueWorker>,
) -> Result<IntegrationAttempt> {
    admit_released_or_replaced(db, merge_service, task, worker, false).await
}

/// [`admit_released`]; with `replace_released` a quarantined attempt with
/// nothing unsettled is superseded (the owner's Retry).
pub(crate) async fn admit_released_or_replaced(
    db: &Arc<SqliteDb>,
    merge_service: &MergeService,
    task: &db::Task,
    worker: Option<&crate::integration_worker::IntegrationQueueWorker>,
    replace_released: bool,
) -> Result<IntegrationAttempt> {
    let deadline = tokio::time::Instant::now() + RELEASE_WAIT;
    loop {
        // Registered before the look: a release between the conflict and
        // the wait below still wakes it.
        let released = worker.map(|worker| worker.released());
        tokio::pin!(released);
        if let Some(released) = released.as_mut().as_pin_mut() {
            released.enable();
        }
        match admit(db, merge_service, task, replace_released).await {
            Err(ServiceError::Db(DbError::VersionConflict))
                if tokio::time::Instant::now() < deadline =>
            {
                if let Some(worker) = worker {
                    worker.notify_answered();
                }
                // The driver's release is the signal; the poll only covers a
                // worker of another process and a release by another writer.
                match released.as_mut().as_pin_mut() {
                    Some(released) => {
                        #[cfg(test)]
                        let poll = if test_poll::disabled(&task.id) {
                            RELEASE_WAIT
                        } else {
                            RELEASE_POLL
                        };
                        #[cfg(not(test))]
                        let poll = RELEASE_POLL;
                        tokio::select! {
                            _ = released => {}
                            _ = tokio::time::sleep(poll) => {}
                        }
                    }
                    None => tokio::time::sleep(RELEASE_POLL).await,
                }
            }
            other => return other,
        }
    }
}

/// The commit the Task's review passed, when the admitted HEAD is a
/// mechanical rebase of it that nothing has checked or reviewed since.
/// `None`: the HEAD is the reviewed commit itself (every ordinary admission).
///
/// The worker merges `original_candidate_sha` without a new check when the
/// target did not move, because that commit is what the review passed. Two
/// admissions hold a HEAD that is not:
///
/// - **A commit an earlier attempt of this Task rebased** and that never
///   merged: the Task left `merging` while that attempt was checking it, or
///   the attempt parked and the owner retried. Admitting it as "the reviewed
///   commit" would fast-forward a commit no check ever passed on. The
///   successor keeps the earlier attempt's reviewed commit, so the head
///   checks the rebased commit again.
/// - **A commit the old merge path rebased before the queue existed**
///   (importer row P6). The review passed on the commit before that rebase,
///   and the old path would have run the review checks on the rebased commit
///   and carried the review. Naming the reviewed commit makes the worker
///   check it and lets `settle` carry the review by the same predicates
///   (one check, no new review).
///
/// A passed review that already covers the HEAD (a re-review of the rebased
/// commit, a stored carry) makes it the reviewed commit again.
async fn reviewed_before_rebase(
    db: &Arc<SqliteDb>,
    superseded: Option<&IntegrationAttempt>,
    task: &db::Task,
    candidate_sha: &str,
) -> Result<Option<String>> {
    let authority = {
        let mut tx = db.pool().begin().await.map_err(DbError::from)?;
        let base = db.attempt_carry_base_in_tx(&mut tx, &task.id).await;
        tx.rollback().await.map_err(DbError::from)?;
        match base {
            Ok(base) => base.map(|base| base.candidate.commit_sha),
            // A fresh review is required anyway; `settle` says so.
            Err(DbError::Check(_)) => None,
            Err(error) => return Err(error.into()),
        }
    };
    if authority.as_deref() == Some(candidate_sha) {
        return Ok(None);
    }
    let rebased_by_queue: Option<String> = db
        .integration_queue_rebased_original_candidate(&task.id, candidate_sha)
        .await?;
    if rebased_by_queue.is_some() {
        return Ok(rebased_by_queue);
    }
    let rebased_before_the_queue = superseded.is_some_and(|attempt| {
        attempt
            .import_source_json
            .as_ref()
            .is_some_and(|source| source["priority"].as_u64() == Some(6))
    });
    Ok(authority.filter(|_| rebased_before_the_queue))
}

/// State `waiting` for a freshly admitted attempt, under the hook step's
/// lease. A refusal (another owner's statement is in force) is logged: the
/// worker's own steps restate the reason as the attempt moves.
pub(crate) async fn state_waiting(db: &Arc<SqliteDb>, attempt: &IntegrationAttempt) -> Result<()> {
    if attempt.state != S::Queued {
        return Ok(());
    }
    let mut tx = db::begin_immediate(db.pool()).await?;
    // A successor may replace a released parked attempt whose statement
    // still owns the condition. Clear only that predecessor, under this
    // Task's lease, before stating the successor in the same transaction.
    if let Some(predecessor) = attempt.predecessor_attempt_id.as_deref() {
        if let Some(task) = db.get_task_in_tx(&mut tx, &attempt.task_ref).await? {
            if task
                .condition
                .integration_reason()
                .is_some_and(|reason| reason.attempt_id().as_str() == predecessor)
            {
                db.state_integration_condition_in_tx(
                    &mut tx,
                    &attempt.task_ref,
                    &ConditionStatement::IntegrationCleared {
                        attempt_id: super::attempt_ref(predecessor),
                    },
                )
                .await?;
            }
        }
    }
    match db
        .state_integration_condition_in_tx(
            &mut tx,
            &attempt.task_ref,
            &ConditionStatement::Integration {
                reason: IntegrationReason::Waiting {
                    attempt_id: super::attempt_ref(&attempt.id),
                },
            },
        )
        .await
    {
        Ok(()) => {
            tx.commit().await?;
            Ok(())
        }
        Err(DbError::Check(reason)) => {
            tracing::warn!(target: "services::integration_steps", task_id = %attempt.task_ref, %reason, "integration waiting statement refused");
            Ok(())
        }
        Err(error) => Err(ServiceError::from(error)),
    }
}

impl crate::TaskService {
    /// Admit a Task that is in a merge state to the integration queue of its
    /// repository's target branch: the write the `run_merge` entry hook
    /// performs, without the hook's legacy-effect reads or its statement.
    /// Returns the Task's current attempt and wakes the queue worker.
    pub async fn admit_to_integration_queue(&self, task_id: &str) -> Result<IntegrationAttempt> {
        let merge_service = self
            .merge_service
            .as_ref()
            .ok_or_else(|| ServiceError::invalid_operation("merge service not configured"))?;
        let task = db::TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        let attempt = admit_released(
            &self.db,
            merge_service,
            &task,
            self.integration_worker().as_deref(),
        )
        .await?;
        if let Some(worker) = self.integration_worker() {
            use crate::integration_worker::IntegrationEnqueuePort;
            worker.notify_enqueued(attempt.queue_id.as_deref().unwrap_or_default());
        }
        Ok(attempt)
    }
}

/// Test-only: Tasks whose admission does not poll for the release of their
/// old head. It then succeeds only when the driver's release wakes it.
#[cfg(test)]
pub(crate) mod test_poll {
    use std::{collections::HashSet, sync::Mutex};
    static DISABLED: Mutex<Option<HashSet<String>>> = Mutex::new(None);
    pub(crate) fn disable(task_id: &str) {
        DISABLED
            .lock()
            .unwrap()
            .get_or_insert_with(HashSet::new)
            .insert(task_id.to_owned());
    }
    pub(super) fn disabled(task_id: &str) -> bool {
        DISABLED
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|tasks| tasks.contains(task_id))
    }
}
