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

/// Admit `task` (in `merging`) to its target's queue and return its current
/// attempt. Idempotent: a hook that runs again for the same status entry,
/// placement and candidate gets the same row.
///
/// An attempt the Task still has from an earlier entry (ejected for repair,
/// sent to review, parked), or one admitted without the owner fields (a
/// shadow row written before the queue was active), is superseded: the
/// successor keeps the Task's place in line (`queue_seq`).
///
/// `VersionConflict` (retried by the hook step with back-off): the earlier
/// attempt still holds its queue slot, or another writer got in between.
pub(crate) async fn admit(
    db: &Arc<SqliteDb>,
    merge_service: &MergeService,
    task: &db::Task,
) -> Result<IntegrationAttempt> {
    let facts = merge_service.integration_admission_facts(task).await?;
    let epoch: i64 = sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=?")
        .bind(&task.id)
        .fetch_one(db.pool())
        .await
        .map_err(DbError::from)?;
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
        attempt.execution_id = Some(facts.execution_id.clone());
        attempt.execution_ref = Some(facts.execution_id.clone());
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
        // already rebased (see `rebased_before_the_queue`).
        attempt.original_candidate_sha =
            Some(reviewed.unwrap_or(&facts.candidate_sha).to_owned());
        if reviewed.is_some() {
            attempt.candidate_sha = Some(facts.candidate_sha.clone());
        }
        attempt
    };
    for _ in 0..3 {
        let Some(current) = db.current_integration_attempt(&task.id).await? else {
            match db
                .admit_integration_attempt(build(format!("admit:{}:{epoch}", task.id), None))
                .await
            {
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
            && (current.original_candidate_sha.as_deref() == Some(facts.candidate_sha.as_str())
                || (current.import_source_json.is_none()
                    && current.candidate_sha.as_deref() == Some(facts.candidate_sha.as_str())
                    && current.state == S::Queued));
        if same_entry && bound(&current) {
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
        let reviewed = rebased_before_the_queue(db, &current, task, &facts.candidate_sha).await;
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
            // Still the head of its queue, or an effect of it is unsettled:
            // the worker releases it (the Task left that entry), then the
            // retried hook supersedes it.
            Err(DbError::InvalidTransition) => return Err(DbError::VersionConflict.into()),
            Err(error) => return Err(error.into()),
        }
    }
    Err(DbError::VersionConflict.into())
}

/// The reviewed commit of a candidate the old merge path rebased before the
/// queue existed (importer row P6: a recorded clean rebase, or the Task
/// between `merging` and its re-review on the `target_moved_rebase` bridge).
///
/// The review passed on the commit before that rebase, and the old path
/// would have run the review checks on the rebased commit and carried the
/// review. Admitting the rebased HEAD as "the reviewed commit" would skip
/// the check and so cost a full review; naming the reviewed commit instead
/// makes the worker check the rebased commit and lets `settle` carry the
/// review by the same predicates the old review entry used. `None` for every
/// other admission, and when no passed review pins a commit.
async fn rebased_before_the_queue(
    db: &Arc<SqliteDb>,
    superseded: &IntegrationAttempt,
    task: &db::Task,
    candidate_sha: &str,
) -> Option<String> {
    let mechanical = superseded
        .import_source_json
        .as_ref()
        .is_some_and(|source| source["priority"].as_u64() == Some(6));
    if !mechanical {
        return None;
    }
    let mut tx = db.pool().begin().await.ok()?;
    let base = db.attempt_carry_base_in_tx(&mut tx, &task.id).await;
    let _ = tx.rollback().await;
    match base {
        Ok(Some(base)) if base.candidate.commit_sha != candidate_sha => {
            Some(base.candidate.commit_sha)
        }
        _ => None,
    }
}

/// State `waiting` for a freshly admitted attempt, under the hook step's
/// lease. A refusal (another owner's statement is in force) is logged: the
/// worker's own steps restate the reason as the attempt moves.
pub(crate) async fn state_waiting(db: &Arc<SqliteDb>, attempt: &IntegrationAttempt) -> Result<()> {
    if attempt.state != S::Queued {
        return Ok(());
    }
    let mut tx = db::begin_immediate(db.pool()).await?;
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
        let attempt = admit(&self.db, merge_service, &task).await?;
        if let Some(worker) = self.integration_worker() {
            use crate::integration_worker::IntegrationEnqueuePort;
            worker.notify_enqueued(attempt.queue_id.as_deref().unwrap_or_default());
        }
        Ok(attempt)
    }
}
