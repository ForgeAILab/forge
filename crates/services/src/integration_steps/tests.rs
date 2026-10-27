//! Every action against a real attempt row and the real step worker, with the
//! queue-worker side scripted (it only ever enqueues a step and moves its own
//! attempt row); one test wires the real `IntegrationQueueWorker`.
use super::*;
use crate::integration_worker::{IntegrationQueueWorker, IntegrationStepPort};
use db::{IntegrationActivationRepo, IntegrationAttempt};
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

mod helpers;
use helpers::*;
pub(super) use helpers::{eventually, World};

#[path = "gate_tests.rs"]
mod gate_tests;

mod cancellation;
mod checks;
mod ci_triage;
mod imports;
mod queue_runtime;
mod results;
mod send_back_cases;

/// Importer rows the queue must not continue by itself, each from the legacy
/// columns the old code wrote, imported twice: the Task keeps its status,
/// nothing merges and no step fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn imported_rows_that_wait_for_their_owner_stay_put_with_a_typed_reason() {
    let conflict = "INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,bridge_kind,bridge_payload,created_at) VALUES('handoff','t','merging','merge_failed','system:workflow','conflict','conflict_handoff','{\"paths\":[\"t.txt\"]}','2026-10-01T00:00:00Z')";
    // (row, Task status, legacy state, attempt state after, typed cause)
    let rows: [(u64, &str, &str, S, Option<&str>); 8] = [
        // P7: a coder is repairing a handed-off conflict.
        (7, "merge_failed", conflict, S::Ejected, None),
        // P13: an existing block stays the Task's reason.
        (
            13,
            "merging",
            "UPDATE task SET blocked_json='{\"reason\":\"held by the owner\"}' WHERE id='t'",
            S::Parked,
            None,
        ),
        // P14: a stored retry deadline is kept; the sweep re-queues at it.
        (
            14,
            "merging",
            "UPDATE task SET metadata_json='{\"deferred_dispatch\":{\"not_before\":\"2099-01-01T00:00:00Z\",\"target_state\":\"merging\"}}' WHERE id='t'",
            S::Parked,
            Some("infrastructure"),
        ),
        // P15: the Task waited for its owner machine.
        (
            15,
            "merging",
            "UPDATE task SET metadata_json='{\"owner_wait\":{\"daemon_id\":\"gone\"}}' WHERE id='t'",
            S::Parked,
            Some("owner_offline"),
        ),
        // P16: the workspace's machine was removed.
        (
            16,
            "merging",
            "UPDATE workspace SET error='machine_removed' WHERE task_id='t'",
            S::Parked,
            Some("owner_required"),
        ),
        // P17: an accepted owner command is still to run.
        (
            17,
            "merging",
            "UPDATE task SET metadata_json='{\"queued_recovery\":{\"id\":\"accepted\"}}' WHERE id='t'",
            S::Parked,
            Some("owner_required"),
        ),
        // P19: an ordinary dirty-worktree repair by the coder.
        (
            19,
            "merge_failed",
            "UPDATE task SET error_annotation='{\"type\":\"dirty_worktree\"}' WHERE id='t'",
            S::Ejected,
            None,
        ),
        // P20: unreadable legacy state is kept, quarantined, for the owner.
        (
            20,
            "merging",
            "UPDATE task SET metadata_json='{corrupt' WHERE id='t'",
            S::Quarantined,
            None,
        ),
    ];
    for (priority, status, seed, state, cause) in rows {
        let row = import_row(status, seed).await;
        assert_eq!(row.priority, priority, "row P{priority}: {}", row.trail);
        assert!(!row.merged, "row P{priority} merged: {}", row.trail);
        assert_eq!(row.state, state, "row P{priority}: {}", row.trail);
        assert_eq!(
            row.status, status,
            "row P{priority}: the Task does not move"
        );
        assert_eq!(row.failed_steps, 0, "row P{priority}");
        // The reason is visible: the typed integration cause the
        // continuation states is on the Task's condition.
        // For these fixtures (legacy columns written with plain SQL) the
        // condition seam refuses the statement: the continuation records the
        // refusal on its step, the Task keeps its own reason, and the
        // reconciler supersedes the row once that reason is lifted. Either
        // the cause is on the Task or the refusal is on the step; never
        // neither.
        if let Some(cause) = cause {
            let stated = row.deferred.as_deref().is_some_and(|deferred| {
                deferred.contains("\"deferred\"") && deferred.contains(&format!("\"{cause}\""))
            });
            assert!(
                stated || row.condition.contains("integration statement refused"),
                "row P{priority}: `deferred` + `{cause}` is neither stated nor recorded as refused: {:?}\n{}",
                row.deferred,
                row.condition
            );
        }
        println!(
            "row P{priority}: status={} attempt={} integration={:?} offers={:?}",
            row.status, row.state, row.deferred, row.offers
        );
        // And the action that moves it is offered. A Task that is being
        // repaired (P7, P19) is in its repair state, where the Worker's own
        // flow continues; every row that stays in the merge state offers its
        // owner an action that gets it moving again.
        if status == "merging" {
            assert!(
                row.offers.iter().any(|(verb, _)| matches!(
                    verb.as_str(),
                    "retry" | "release" | "restart" | "approve"
                )),
                "row P{priority}: no action is offered that moves the Task: {:?}\n{}",
                row.offers,
                row.condition
            );
        }
    }
}
